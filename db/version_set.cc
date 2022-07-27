// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_set.h"

#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include <algorithm>
#include <cstdio>

#include "leveldb/env.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"

#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"

#include "ac-key/arc_cache.h"
#include "ac-key/cache_format.h"
#include "blob/basic_cache.h"

namespace leveldb {

static size_t TargetFileSize(const Options* options) {
  return options->max_file_size;
}

// Maximum bytes of overlaps in grandparent (i.e., level+2) before we
// stop building a single file in a level->level+1 compaction.
static int64_t MaxGrandParentOverlapBytes(const Options* options) {
  return 10 * TargetFileSize(options);
}

// Maximum number of bytes in all compacted files.  We avoid expanding
// the lower level file set of a compaction if it would make the
// total compaction cover more than this many bytes.
static int64_t ExpandedCompactionByteSizeLimit(const Options* options) {
  return 25 * TargetFileSize(options);
}

static double MaxBytesForLevel(const Options* options, int level) {
  // Note: the result for level zero is not really used since we set
  // the level-0 compaction threshold based on number of files.

  // Result for both level-0 and level-1
  double result = 10. * 1048576.0;
  while (level > 1) {
    result *= 10;
    level--;
  }
  return result;
}

static uint64_t MaxFileSizeForLevel(const Options* options, int level) {
  // We could vary per level to reduce number of files?
  return TargetFileSize(options);
}

static int64_t TotalFileSize(const std::vector<FileMetaData*>& files) {
  int64_t sum = 0;
  for (size_t i = 0; i < files.size(); i++) {
    sum += files[i]->file_size;
  }
  return sum;
}

Version::~Version() {
  assert(refs_ == 0);

  // Remove from linked list
  prev_->next_ = next_;
  next_->prev_ = prev_;

  // Drop references to files
  for (int level = 0; level < config::kNumLevels; level++) {
    for (size_t i = 0; i < files_[level].size(); i++) {
      FileMetaData* f = files_[level][i];
      assert(f->refs > 0);
      f->refs--;
      if (f->refs <= 0) {
        delete f;
      }
    }
  }
}

int FindGuard(const InternalKeyComparator& icmp,
              const std::vector<GuardMetaData*> guards, const Slice& key) {
  int size = static_cast<int>(guards.size());
  if (size == 0) {
    return -1;
  }

  auto pos =
      std::lower_bound(guards.begin(), guards.end(), key,
                       [icmp](const GuardMetaData* guard, const Slice& key2) {
                         return icmp.InternalKeyComparator::Compare(
                                    guard->guard_key.Encode(), key2) < 0;
                       });
  if (pos == guards.end()) {
    return size - 1;
  } else {
    int index = pos - guards.begin();
    int r = icmp.InternalKeyComparator::Compare(
        guards[index]->guard_key.Encode(), key);
    if (r > 0) {
      if (index == 0) {
        return -1;
      } else {
        return index - 1;
      }
    }
  }

  return pos - guards.begin();
}

int FindFile(const InternalKeyComparator& icmp,
             const std::vector<FileMetaData*>& files, const Slice& key,
             bool use_user_comparator) {
  uint32_t left = 0;
  uint32_t right = files.size();

  const Comparator* user_cmp = icmp.user_comparator();
  while (left < right) {
    uint32_t mid = (left + right) / 2;
    const FileMetaData* f = files[mid];
    if (use_user_comparator) {
      if (user_cmp->Compare(f->largest.user_key(), key) < 0) {
        left = mid + 1;
      } else {
        right = mid;
      }
    } else {
      if (icmp.InternalKeyComparator::Compare(f->largest.Encode(), key) < 0) {
        // Key at "mid.largest" is < "target".  Therefore all
        // files at or before "mid" are uninteresting.
        left = mid + 1;
      } else {
        // Key at "mid.largest" is >= "target".  Therefore all files
        // after "mid" are uninteresting.
        right = mid;
      }
    }
  }
  return right;
}

static bool AfterFile(const Comparator* ucmp, const Slice* user_key,
                      const FileMetaData* f) {
  // null user_key occurs before all keys and is therefore never after *f
  return (user_key != nullptr &&
          ucmp->Compare(*user_key, f->largest.user_key()) > 0);
}

static bool BeforeFile(const Comparator* ucmp, const Slice* user_key,
                       const FileMetaData* f) {
  // null user_key occurs after all keys and is therefore never before *f
  return (user_key != nullptr &&
          ucmp->Compare(*user_key, f->smallest.user_key()) < 0);
}

bool SomeFileOverlapsRange(const InternalKeyComparator& icmp, bool is_level0,
                           const std::vector<FileMetaData*>& sentinels_file,
                           const std::vector<GuardMetaData*>& guards,
                           const Slice* smallest_user_key,
                           const Slice* largest_user_key) {
  const Comparator* ucmp = icmp.user_comparator();
  if (!is_level0) {
    // Need to check against all files
    for (size_t i = 0; i < sentinels_file.size(); i++) {
      const FileMetaData* f = sentinels_file[i];
      if (AfterFile(ucmp, smallest_user_key, f) ||
          BeforeFile(ucmp, largest_user_key, f)) {
        // No overlap
      } else {
        return true;  // Overlap
      }
    }
    return false;
  }
  int index = 0;
  int guard_index = 0;
  bool in_sentinel = false;
  if (smallest_user_key != nullptr) {
    // Find the earliest possible internal key for smallest_user_key
    InternalKey small_key(*smallest_user_key, kMaxSequenceNumber,
                          kValueTypeForSeek);
    index = FindGuard(icmp, guards, small_key.Encode());

    if (index == -1) {
      in_sentinel = true;
      index = FindFile(icmp, sentinels_file, small_key.Encode());
    } else {
      guard_index = index;
      index = FindFile(icmp, guards[index]->files_meta, small_key.Encode());
    }
  }
  if (in_sentinel) {
    if (index >= sentinels_file.size()) {
      return false;
    }
    return !BeforeFile(ucmp, largest_user_key, sentinels_file[index]);
  }
  if (index >= guards[index]->files_meta.size()) {
    return false;
  }
  return !BeforeFile(ucmp, largest_user_key,
                     guards[guard_index]->files_meta[index]);
}

bool SomeFileOverlapsRange(const InternalKeyComparator& icmp,
                           bool disjoint_sorted_files,
                           const std::vector<FileMetaData*>& files,
                           const Slice* smallest_user_key,
                           const Slice* largest_user_key) {
  const Comparator* ucmp = icmp.user_comparator();
  if (!disjoint_sorted_files) {
    // Need to check against all files
    for (size_t i = 0; i < files.size(); i++) {
      const FileMetaData* f = files[i];
      if (AfterFile(ucmp, smallest_user_key, f) ||
          BeforeFile(ucmp, largest_user_key, f)) {
        // No overlap
      } else {
        return true;  // Overlap
      }
    }
    return false;
  }

  // Binary search over file list
  uint32_t index = 0;
  if (smallest_user_key != nullptr) {
    // Find the earliest possible internal key for smallest_user_key
    InternalKey small_key(*smallest_user_key, kMaxSequenceNumber,
                          kValueTypeForSeek);
    index = FindFile(icmp, files, small_key.Encode());
  }

  if (index >= files.size()) {
    // beginning of range is after all files, so no overlap.
    return false;
  }

  return !BeforeFile(ucmp, largest_user_key, files[index]);
}

class Version::LevelGuardNumIterator : public Iterator {
 public:
  using Vec = std::vector<std::pair<uint64_t, uint64_t>>;

  LevelGuardNumIterator(const InternalKeyComparator& icmp,
                        const std::vector<FileMetaData*>* sentinel_files,
                        const std::vector<GuardMetaData*>* guards)
      : icmp_(icmp),
        sentinel_files_(sentinel_files),
        guards_(guards),
        guard_index_(1 + guards_->size()) {}  //置于无效位置

  bool Valid() const override {
    return guard_index_ >= 0 && guard_index_ < 1 + guards_->size();
  }

  void SeekToFirst() override {
    guard_index_ = 0;
    DecodeFileMetaInBuffer(nullptr);
  }
  void SeekToLast() override {
    if (!guards_->empty()) {
      guard_index_ = guards_->size();
    } else {
      guard_index_ = 0;
    }
    DecodeFileMetaInBuffer(nullptr);
  }
  void Seek(const Slice& target) override {
    guard_index_ = FindGuard(icmp_, *guards_, target);
    if (guard_index_ == -1) {
      guard_index_ = 0;  //在sentinel_files中
    }
    DecodeFileMetaInBuffer(&target);
  }
  void Next() override { guard_index_++; }
  void Prev() override { guard_index_--; }

  Slice key() const override {}
  Slice value() const override { return FormatFileMetaBuffer(); }

  Status status() const override { return Status::OK(); }
  uint64_t GetBlobNumber() const override { return kInValidBlobFileNumber; }
  uint64_t GetBlobSize() const override { return kInValidBlobFileSize; }

  static Vec GetFileMetaList(const Slice& buf) {
    Vec result;
    uint64_t count = DecodeFixed64(buf.data());
    if (count == 0) {
      return result;
    }
    const char* start = buf.data() + sizeof(uint64_t);
    while (count--) {
      result.push_back(
          {DecodeFixed64(start), DecodeFixed64(start + sizeof(uint64_t))});
      start += 2 * sizeof(uint64_t);
    }
    return result;
  }

  int32_t GuardIndex() const { return guard_index_; }

 private:
  const InternalKeyComparator icmp_;
  const std::vector<FileMetaData*>* const sentinel_files_;
  const std::vector<GuardMetaData*>* const guards_;
  int32_t guard_index_;  //指示在哪一个guard中 guard_index==0 在sentinel_files中

  void DecodeFileMetaInBuffer(const Slice* target) {
    const std::vector<FileMetaData*>* file_list = nullptr;

    if (guard_index_ == 0) {
      file_list = sentinel_files_;
    } else {
      file_list = &(*guards_)[guard_index_]->files_meta;
    }

    for (size_t i = 0; i < file_list->size(); ++i) {
      const FileMetaData* meta = (*file_list)[i];
      if (target == nullptr) {
        value_buf_.push_back({meta->number, meta->file_size});
      } else {
        if (icmp_.Compare(meta->smallest.Encode(), *target) <= 0 &&
            icmp_.Compare(meta->largest.Encode(), *target) >= 0) {
          value_buf_.push_back({meta->number, meta->file_size});
        }
      }
    }

    if (!value_buf_.empty()) {
      value_buf_.push_back({value_buf_.size(), 0});
    }

    if (target != nullptr) {  //需要越过sentinel_files
      if (guard_index_ != 0) {
        ++guard_index_;
      }
    }
  }
  // format file_count->[file_number:file_size]->[file_number->file_size]
  Slice FormatFileMetaBuffer() const {
    auto pos = value_buf_.rbegin();

    if (pos != value_buf_.rend()) {
      PutFixed64(&result_, pos->first);
      ++pos;
    }

    for (; pos != value_buf_.rend(); ++pos) {
      PutFixed64(&result_, pos->first);
      PutFixed64(&result_, pos->second);
    }
    return result_;
  }

  mutable std::string result_;  //暂存文件元信息的编码结果
  //记录着可能存在目标key的文件的file_number以及file_size
  //第一个参数为文件号第二个为文件大小 最后一个参数为总文件数量
  mutable Vec value_buf_;
};

// An internal iterator.  For a given version/level pair, yields
// information about the files in the level.  For a given entry, key()
// is the largest key that occurs in the file, and value() is an
// 16-byte value containing the file number and file size, both
// encoded using EncodeFixed64.
class Version::LevelFileNumIterator : public Iterator {
 public:
  LevelFileNumIterator(const InternalKeyComparator& icmp,
                       const std::vector<FileMetaData*>* flist)
      : icmp_(icmp), flist_(flist), index_(flist->size()) {  // Marks as invalid
  }
  bool Valid() const override { return index_ < flist_->size(); }
  void Seek(const Slice& target) override {
    index_ = FindFile(icmp_, *flist_, target);
  }
  void SeekToFirst() override { index_ = 0; }
  void SeekToLast() override {
    index_ = flist_->empty() ? 0 : flist_->size() - 1;
  }
  void Next() override {
    assert(Valid());
    index_++;
  }
  void Prev() override {
    assert(Valid());
    if (index_ == 0) {
      index_ = flist_->size();  // Marks as invalid
    } else {
      index_--;
    }
  }
  Slice key() const override {
    assert(Valid());
    return (*flist_)[index_]->largest.Encode();
  }
  Slice value() const override {
    assert(Valid());
    EncodeFixed64(value_buf_, (*flist_)[index_]->number);
    EncodeFixed64(value_buf_ + 8, (*flist_)[index_]->file_size);
    return Slice(value_buf_, sizeof(value_buf_));
  }

  Iterator* current() const override { return nullptr; }

  uint64_t GetBlobNumber() const override { return 0; }

  uint64_t GetBlobSize() const override { return 0; }

  Status status() const override { return Status::OK(); }

  void SetIterType(bool is_mem_iter) override {}

  bool IsMemIter() const override{};

 private:
  const InternalKeyComparator icmp_;
  const std::vector<FileMetaData*>* const flist_;
  uint32_t index_;

  // Backing store for value().  Holds the file number and size.
  mutable char value_buf_[16];
};

static Iterator* GetFileIterator(void* arg, const ReadOptions& options,
                                 const Slice& file_value) {
  TableCache* cache = reinterpret_cast<TableCache*>(arg);
  if (file_value.size() != 16) {
    return NewErrorIterator(
        Status::Corruption("FileReader invoked with unexpected value"));
  } else {
    return cache->NewIterator(options, DecodeFixed64(file_value.data()),
                              DecodeFixed64(file_value.data() + 8));
  }
}

Iterator* GetGuardFileIterator(void* arg, const ReadOptions& options,
                               const Slice& value_buf) {
  VersionSet::PackCacheAndComparator* packing =
      reinterpret_cast<VersionSet::PackCacheAndComparator*>(arg);
  assert(packing);
  TableCache* cache = packing->cache_;
  assert(cache);
  const InternalKeyComparator* icmp = packing->icmp_;
  assert(icmp);

  Version::LevelGuardNumIterator::Vec file_list =
      Version::LevelGuardNumIterator::GetFileMetaList(value_buf);
  const int iter_size = file_list.size();
  int num = 0;

  Iterator** iter = new Iterator*[iter_size];
  for (auto& [file_number, file_size] : file_list) {
    iter[num] = cache->NewIterator(options, file_number, file_size);
    if (!iter[num++]->status().ok()) {
      delete[] iter;
      return NewErrorIterator(Status::Corruption("iterator is empty"));
    }
  }
  Iterator* result = NewMergingIterator(icmp, iter, iter_size);
  delete[] iter;
  return result;
}

Iterator* Version::NewConcatenatingIterator(const ReadOptions& options,
                                            int level) const {
  return NewTwoLevelIterator(
      new LevelFileNumIterator(vset_->icmp_, &files_[level]), &GetFileIterator,
      vset_->table_cache_, options);
}

Iterator* Version::NewLevelGuardIterator(const ReadOptions& options,
                                         int level) const {
  return NewTwoLevelIterator(
      new LevelGuardNumIterator(vset_->icmp_, &sentinel_files_[level],
                                &guards_[level]),
      &GetGuardFileIterator, &vset_->pack_cache_and_comparator_, options);
}

void Version::AddIterators(const ReadOptions& options,
                           std::vector<Iterator*>* iters) {
  // Merge all level zero files together since they may overlap
  for (size_t i = 0; i < sentinel_files_[0].size(); i++) {
    iters->push_back(vset_->table_cache_->NewIterator(
        options, files_[0][i]->number, files_[0][i]->file_size));
  }

  for (int level = 1; level < config::kNumLevels; ++level) {
    if (sentinel_files_[level].empty() && guards_[level].empty()) {
      continue;
    }
    iters->push_back(NewLevelGuardIterator(options, level));
  }
}

static void GetValueState(void* arg, const Slice& ikey, const Slice& v) {
  Saver* s = reinterpret_cast<Saver*>(arg);
  ParsedInternalKey parsed_key;
  if (!ParseInternalKey(ikey, &parsed_key)) {
    s->state = kCorrupt;
  } else {
    if (s->ucmp->Compare(parsed_key.user_key, s->user_key) == 0) {
      if (v.data()[0] == KVSeparation::kNoSeparation) {
        s->value->assign(v.data() + 1, v.size() - 1);
      } else if (v.data()[0] == KVSeparation::kSeparation) {
        s->blob_offset = Slice(v.data() + 1, v.size() - 1);
      }
      s->state = (parsed_key.type == kTypeValue) ? kFound : kDeleted;
    }
  }
}

static bool NewestFirst(FileMetaData* a, FileMetaData* b) {
  return a->number > b->number;
}

static void GetBlobFileNumber(uint64_t* blob_number, const Slice& v) {
  *blob_number = DecodeFixed64(v.data());
}

void Version::ForEachOverlapping(Slice user_key, Slice internal_key, void* arg,
                                 bool (*func)(void*, int, FileMetaData*)) {
  const Comparator* ucmp = vset_->icmp_.user_comparator();

  // Search level-0 in order from newest to oldest.
  std::vector<FileMetaData*> tmp;
  tmp.reserve(sentinel_files_[0].size());
  for (uint32_t i = 0; i < sentinel_files_[0].size(); i++) {
    FileMetaData* f = sentinel_files_[0][i];
    if (ucmp->Compare(user_key, f->smallest.user_key()) >= 0 &&
        ucmp->Compare(user_key, f->largest.user_key()) <= 0) {
      tmp.push_back(f);
    }
  }
  if (!tmp.empty()) {
    std::sort(tmp.begin(), tmp.end(), NewestFirst);
    for (uint32_t i = 0; i < tmp.size(); i++) {
      if (!(*func)(arg, 0, tmp[i])) {
        return;
      }
    }
  }

  for (int level = 1; level < config::kNumLevels; ++level) {
    size_t num_of_guards = guards_[level].size();

    int index = FindGuard(vset_->icmp_, guards_[level], internal_key);
    if (index == -1) {
      size_t sentinel_size = sentinel_files_[level].size();

      for (size_t i = 0; i < sentinel_size; ++i) {
        if (ucmp->Compare(user_key,
                          sentinel_files_[level][i]->smallest.user_key()) >=
                0 &&
            ucmp->Compare(user_key,
                          sentinel_files_[level][i]->largest.user_key()) <= 0) {
          if (!(*func)(arg, level, sentinel_files_[level][i])) {
            return;
          }
        }
      }
    } else {
      size_t file_size = guards_[level][index]->files_meta.size();
      for (size_t i = 0; i < file_size; ++i) {
        FileMetaData* file_meta = guards_[level][index]->files_meta[i];
        if (ucmp->Compare(user_key, file_meta->smallest.user_key()) >= 0 &&
            ucmp->Compare(user_key, file_meta->largest.user_key()) <= 0) {
          if (!(*func)(arg, level, file_meta)) {
            return;
          }
        }
      }
    }
  }
}

Status Version::Get(const ReadOptions& options, const LookupKey& k,
                    std::string* value, GetStats* stats) {
  stats->seek_file = nullptr;
  stats->seek_file_level = -1;

  struct State {
    Saver saver;
    GetStats* stats;
    const ReadOptions* options;
    Slice ikey;
    FileMetaData* last_file_read;
    int last_file_read_level;

    VersionSet* vset;
    Status s;
    bool found;

    static Status SearchInBlobCache(void* arg, uint64_t file_number,
                                    uint64_t file_size) {
      State* state = reinterpret_cast<State*>(arg);
      state->s = state->vset->blob_cache_->Get(
          *state->options, file_number, file_size, state->saver.blob_offset,
          &state->saver, SaveValue);
      return state->s;
    }

    static void SearchInAdaptiveCache(State* state, const Slice& internal_key,
                                      HitCacheType* hit_cache_type, int level) {
      Cache* cache = state->vset->adaptive_cache_;
      Cache::Handle* handle = cache->Lookup(internal_key);
      LRUHandle* lru_handle = reinterpret_cast<LRUHandle*>(handle);

      if (handle != nullptr) {
        std::string* value =
            reinterpret_cast<std::string*>(cache->Value(handle));

        if (lru_handle->hit_cache_type == HitCacheType::kKVRealCache) {
          state->saver.value->assign(value->data(), value->size());
          cache->Release(handle);
        } else if (lru_handle->hit_cache_type ==
                   HitCacheType::kKPRealCache) {  //击中kpcache
                                                  //提升kpcache为kvcache
          uint64_t blob_number = 0;
          uint64_t blob_file_size = 0;

          DecodeKPValue(value, &blob_number, &blob_file_size,
                        &state->saver.blob_offset);

          SearchInBlobCache(state, blob_number, blob_file_size);

          std::string* value = new std::string(*state->saver.value);
          double caching_factor = CalculateCachingFactor(
              level, HandleType::kKVHandle, value->size());

          cache->Release(handle);
          handle = cache->Insert(internal_key, reinterpret_cast<void*>(value),
                                 1, DeleteHandle, HandleType::kKVHandle,
                                 caching_factor);
          cache->Release(handle);
        }

        *hit_cache_type = lru_handle->hit_cache_type;
      } else {
        *hit_cache_type = HitCacheType::kNoHitCache;
      }
    }

    static void InsertToCacheByHitType(State* state,
                                       const HitCacheType& hit_cache_type,
                                       int level) {
      Cache* cache = state->vset->adaptive_cache_;
      Cache::Handle* handle = nullptr;

      if (hit_cache_type == HitCacheType::kKPGhostCache ||
          hit_cache_type == HitCacheType::kKVGhostCache) {
        std::string* value = new std::string(*state->saver.value);
        double caching_factor =
            CalculateCachingFactor(level, HandleType::kKVHandle, value->size());

        handle = cache->Insert(state->ikey, value, 1, DeleteHandle,
                               HandleType::kKVHandle, caching_factor);
      } else {
        std::string encode_value;
        EncodeKPValue(&encode_value, state->saver.blob_number,
                      state->saver.blob_size,
                      DecodeFixed64(state->saver.blob_offset.data()));
        std::string* value = new std::string(encode_value);
        double caching_factor =
            CalculateCachingFactor(level, HandleType::kKPHandle, value->size());

        handle =
            cache->Insert(state->ikey, reinterpret_cast<void*>(value), 1,
                          DeleteHandle, HandleType::kKPHandle, caching_factor);
      }
      cache->Release(handle);
    }

    static bool Match(void* arg, int level, FileMetaData* f) {
      State* state = reinterpret_cast<State*>(arg);

      if (state->stats->seek_file == nullptr &&
          state->last_file_read != nullptr) {
        // We have had more than one seek for this read.  Charge the 1st file.
        state->stats->seek_file = state->last_file_read;
        state->stats->seek_file_level = state->last_file_read_level;
      }

      HitCacheType hit_cache_type = HitCacheType::kNoHitCache;
      SearchInAdaptiveCache(state, state->ikey, &hit_cache_type, level);

      //没有查到或者击中幽灵缓存 需要走sst->blob
      if (hit_cache_type != HitCacheType::kKVRealCache &&
          hit_cache_type != HitCacheType::kKPRealCache) {
        state->last_file_read = f;
        state->last_file_read_level = level;
        state->s = state->vset->table_cache_->Get(*state->options, f->number,
                                                  f->file_size, state->ikey,
                                                  &state->saver, GetValueState);

        if (state->saver.state == kFound) {
          if (state->saver.blob_offset.compare(Slice()) != 0) {  //进行了KV分离
            SearchInBlobCache(reinterpret_cast<void*>(state),
                              state->saver.blob_number, state->saver.blob_size);
            InsertToCacheByHitType(state, hit_cache_type, level);
          }  //对于没有进行KV分离的k-v对不需要缓存在kpcache中,因为无论是否缓存都需要读取sst进行磁盘I/O
        }
      }

      if (!state->s.ok()) {
        state->found = true;
        return false;
      }
      switch (state->saver.state) {
        case kNotFound:
          return true;  // Keep searching in other files
        case kFound:
          state->found = true;
          return false;
        case kDeleted:
          return false;
        case kCorrupt:
          state->s =
              Status::Corruption("corrupted key for ", state->saver.user_key);
          state->found = true;
          return false;
      }

      // Not reached. Added to avoid false compilation warnings of
      // "control reaches end of non-void function".
      return false;
    }
  };

  State state;
  state.found = false;
  state.stats = stats;
  state.last_file_read = nullptr;
  state.last_file_read_level = -1;

  state.options = &options;
  state.ikey = k.internal_key();
  state.vset = vset_;

  state.saver.state = kNotFound;
  state.saver.ucmp = vset_->icmp_.user_comparator();
  state.saver.user_key = k.user_key();
  state.saver.value = value;
  state.saver.blob_offset = Slice();

  ForEachOverlapping(state.saver.user_key, state.ikey, &state, &State::Match);

  return state.found ? state.s : Status::NotFound(Slice());
}

bool Version::UpdateStats(const GetStats& stats) {
  FileMetaData* f = stats.seek_file;
  if (f != nullptr) {
    f->allowed_seeks--;
    if (f->allowed_seeks <= 0 && file_to_compact_ == nullptr) {
      file_to_compact_ = f;
      file_to_compact_level_ = stats.seek_file_level;
      return true;
    }
  }
  return false;
}

bool Version::RecordReadSample(Slice internal_key) {
  ParsedInternalKey ikey;
  if (!ParseInternalKey(internal_key, &ikey)) {
    return false;
  }

  struct State {
    GetStats stats;  // Holds first matching file
    int matches;

    static bool Match(void* arg, int level, FileMetaData* f) {
      State* state = reinterpret_cast<State*>(arg);
      state->matches++;
      if (state->matches == 1) {
        // Remember first match.
        state->stats.seek_file = f;
        state->stats.seek_file_level = level;
      }
      // We can stop iterating once we have a second match.
      return state->matches < 2;
    }
  };

  State state;
  state.matches = 0;
  ForEachOverlapping(ikey.user_key, internal_key, &state, &State::Match);

  // Must have at least two matches since we want to merge across
  // files. But what if we have a single file that contains many
  // overwrites and deletions?  Should we have another mechanism for
  // finding such files?
  if (state.matches >= 2) {
    // 1MB cost is about 1 seek (see comment in Builder::Apply).
    return UpdateStats(state.stats);
  }
  return false;
}

void Version::Ref() { ++refs_; }

void Version::Unref() {
  assert(this != &vset_->dummy_versions_);
  assert(refs_ >= 1);
  --refs_;
  if (refs_ == 0) {
    delete this;
  }
}

bool Version::OverlapInLevel(int level, const Slice* smallest_user_key,
                             const Slice* largest_user_key) {
  return SomeFileOverlapsRange(vset_->icmp_, (level > 0),
                               sentinel_files_[level], guards_[level],
                               smallest_user_key, largest_user_key);
}

void Version::GetOverlappingWithSentinelFiles(
    int level, const InternalKey* begin, const InternalKey* end,
    std::vector<FileMetaData*>* inputs) {
  for (size_t i = 0; i < sentinel_files_[level].size(); ++i) {
    const InternalKey& smallest = sentinel_files_[level][i]->smallest;
    const InternalKey& largest = sentinel_files_[level][i]->largest;
    if (begin != nullptr && vset_->icmp_.InternalKeyComparator::Compare(
                                begin->Encode(), largest.Encode()) > 0) {
    } else if (end != nullptr && vset_->icmp_.InternalKeyComparator::Compare(
                                     end->Encode(), smallest.Encode()) < 0) {
    } else {
      inputs->emplace_back(sentinel_files_[level][i]);
    }
  }
}

void Version::GetOverlappingWithGuards(int level, const InternalKey* begin,
                                       const InternalKey* end,
                                       std::vector<GuardMetaData*>* inputs) {
  for (size_t i = 0; i < guards_[level].size(); ++i) {
    GuardMetaData* guard = guards_[level][i];
    const std::vector<FileMetaData*>& files = guard->files_meta;
    for (size_t i = 0; i < files.size(); ++i) {
      const InternalKey& smallest = files[i]->smallest;
      const InternalKey& largest = files[i]->largest;
      if (begin != nullptr && vset_->icmp_.InternalKeyComparator::Compare(
                                  begin->Encode(), largest.Encode()) > 0) {
      } else if (end != nullptr && vset_->icmp_.InternalKeyComparator::Compare(
                                       end->Encode(), smallest.Encode()) < 0) {
      } else {
        inputs->emplace_back(guard);
        break;
      }
    }
  }
}

void Version::GetOverlappingInputs(int level, const InternalKey* begin,
                                   const InternalKey* end,
                                   std::vector<FileMetaData*>* sentinel_files,
                                   std::vector<GuardMetaData*>* guards) {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  sentinel_files->clear();
  guards->clear();

  GetOverlappingWithSentinelFiles(level, begin, end, sentinel_files);
  GetOverlappingWithGuards(level, begin, end, guards);
}

std::string Version::DebugString() const {
  std::string r;
  for (int level = 0; level < config::kNumLevels; level++) {
    // E.g.,
    //   --- level 1 ---
    //   17:123['a' .. 'd']
    //   20:43['e' .. 'g']
    r.append("--- level ");
    AppendNumberTo(&r, level);
    r.append(" ---\n");
    const std::vector<FileMetaData*>& files = files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      r.push_back(' ');
      AppendNumberTo(&r, files[i]->number);
      r.push_back(':');
      AppendNumberTo(&r, files[i]->file_size);
      r.append("[");
      r.append(files[i]->smallest.DebugString());
      r.append(" .. ");
      r.append(files[i]->largest.DebugString());
      r.append("]\n");
    }
  }
  return r;
}

// A helper class so we can efficiently apply a whole sequence
// of edits to a particular state without creating intermediate
// Versions that contain full copies of the intermediate state.
class VersionSet::Builder {
 private:
  // Helper to sort by v->files_[file_number].smallest
  struct BySmallestKey {
    const InternalKeyComparator* internal_comparator;

    bool operator()(FileMetaData* f1, FileMetaData* f2) const {
      int r = internal_comparator->Compare(f1->smallest, f2->smallest);
      if (r != 0) {
        return (r < 0);
      } else {
        // Break ties by file number
        return (f1->number < f2->number);
      }
    }
  };

  struct BySmallestGuardKey {
    const InternalKeyComparator* internal_comparator;

    bool operator()(GuardMetaData* guard1, GuardMetaData* guard2) const {
      return internal_comparator->Compare(guard1->guard_key,
                                          guard2->guard_key) < 0;
    }
  };

  struct BySmallestInternalKey {
    const InternalKeyComparator* internal_comparactor;
    bool operator()(InternalKey key1, InternalKey key2) const {
      return internal_comparactor->Compare(key1, key2) < 0;
    }
  };

  typedef std::set<FileMetaData*, BySmallestKey> FileSet;
  typedef std::set<GuardMetaData*, BySmallestGuardKey> GuardSet;
  struct LevelState {
    std::set<uint64_t> deleted_files;
    std::set<InternalKey, BySmallestInternalKey> deleted_guards;
    FileSet* added_files;
    GuardSet* added_guards_;
    GuardSet* added_uncommitted_guards_;
  };

  VersionSet* vset_;
  Version* base_;
  LevelState levels_[config::kNumLevels];

  void UnrefGuard(std::vector<GuardMetaData*>* guards) {
    for (size_t i = 0; i < guards->size(); i++) {
      GuardMetaData* guard = (*guards)[i];
      guard->refs--;
      if (guard->refs <= 0) {
        delete guard;
      }
    }
  }

  void UnrefFile(std::vector<FileMetaData*>* files) {
    for (size_t i = 0; i < files->size(); i++) {
      FileMetaData* f = (*files)[i];
      f->refs--;
      if (f->refs <= 0) {
        delete f;
      }
    }
  }

 public:
  // Initialize a builder with the files from *base and other info from
  // *vset
  Builder(VersionSet* vset, Version* base) : vset_(vset), base_(base) {
    base_->Ref();
    BySmallestKey cmp;
    BySmallestGuardKey guard_cmp;
    cmp.internal_comparator = &vset_->icmp_;
    guard_cmp.internal_comparator = &vset_->icmp_;
    for (int level = 0; level < config::kNumLevels; level++) {
      levels_[level].added_files = new FileSet(cmp);
      levels_[level].added_guards_ = new GuardSet(guard_cmp);
      levels_[level].added_uncommitted_guards_ = new GuardSet(guard_cmp);
    }
  }

  ~Builder() {
    for (int level = 0; level < config::kNumLevels; level++) {
      const FileSet* added = levels_[level].added_files;
      const GuardSet* added_guard = levels_[level].added_guards_;
      const GuardSet* added_uncommitted_guard =
          levels_[level].added_uncommitted_guards_;
      assert(added);
      assert(added_guard);
      assert(added_uncommitted_guard);

      std::vector<FileMetaData*> to_unref;
      std::vector<GuardMetaData*> guard_to_unref, uncommitted_guard_to_unref;
      to_unref.reserve(added->size());
      guard_to_unref.reserve(added_guard->size());
      uncommitted_guard_to_unref.reserve(added_uncommitted_guard->size());
      for (FileSet::const_iterator it = added->begin(); it != added->end();
           ++it) {
        to_unref.push_back(*it);
      }
      for (GuardSet::const_iterator it = added_guard->begin();
           it != added_guard->end(); ++it) {
        guard_to_unref.push_back(*it);
      }
      for (GuardSet::const_iterator it = added_uncommitted_guard->begin();
           it != added_uncommitted_guard->end(); ++it) {
        uncommitted_guard_to_unref.push_back(*it);
      }
      delete added;
      delete added_guard;
      delete added_uncommitted_guard;
      added = nullptr;
      added_guard = nullptr;
      added_uncommitted_guard = nullptr;
      UnrefFile(&to_unref);
      UnrefGuard(&guard_to_unref);
      UnrefGuard(&uncommitted_guard_to_unref);
    }

    base_->Unref();
  }

  // Apply all of the edits in *edit to the current state.
  void Apply(VersionEdit* edit) {
    // Update compaction pointers
    for (size_t i = 0; i < edit->compact_pointers_.size(); i++) {
      const int level = edit->compact_pointers_[i].first;
      vset_->compact_pointer_[level] =
          edit->compact_pointers_[i].second.Encode().ToString();
    }

    // Delete files
    for (const auto& deleted_file_set_kvp : edit->deleted_files_) {
      const int level = deleted_file_set_kvp.first;
      const uint64_t number = deleted_file_set_kvp.second;
      levels_[level].deleted_files.insert(number);
    }

    // Add new files
    for (size_t i = 0; i < edit->new_files_.size(); i++) {
      const int level = edit->new_files_[i].first;
      FileMetaData* f = new FileMetaData(edit->new_files_[i].second);
      f->refs = 1;

      // We arrange to automatically compact this file after
      // a certain number of seeks.  Let's assume:
      //   (1) One seek costs 10ms
      //   (2) Writing or reading 1MB costs 10ms (100MB/s)
      //   (3) A compaction of 1MB does 25MB of IO:
      //         1MB read from this level
      //         10-12MB read from next level (boundaries may be misaligned)
      //         10-12MB written to next level
      // This implies that 25 seeks cost the same as the compaction
      // of 1MB of data.  I.e., one seek costs approximately the
      // same as the compaction of 40KB of data.  We are a little
      // conservative and allow approximately one seek for every 16KB
      // of data before triggering a compaction.
      f->allowed_seeks = static_cast<int>((f->file_size / 16384U));
      if (f->allowed_seeks < 100) f->allowed_seeks = 100;

      levels_[level].deleted_files.erase(f->number);
      levels_[level].added_files->insert(f);
    }

    for (size_t i = 0; i < edit->new_guards_->size(); ++i) {
      const std::vector<GuardMetaData>& guards = edit->new_guards_[i];
      for (size_t j = 0; j < guards.size(); ++j) {
        const int level = guards[j].level;
        GuardMetaData* guard = new GuardMetaData(guards[j]);
        levels_[level].added_guards_->insert(guard);
      }
    }

    for (size_t i = 0; i < edit->new_uncommitted_guards_->size(); ++i) {
      const std::vector<GuardMetaData>& guards =
          edit->new_uncommitted_guards_[i];
      for (size_t j = 0; j < guards.size(); ++j) {
        const int level = guards[j].level;
        GuardMetaData* guard = new GuardMetaData(guards[j]);
        levels_[level].added_uncommitted_guards_->insert(guard);
      }
    }
  }

  void SaveUnCommittedGuard(Version* v) { SaveGuardMeta(v, true); }
  void SaveGuard(Version* v) { SaveGuardMeta(v, false); }

  void SaveGuardMeta(Version* v, bool save_uncommitted_guard) {
    BySmallestGuardKey guard_cmp;
    guard_cmp.internal_comparator = &vset_->icmp_;
    std::vector<GuardMetaData*>* real_guards = nullptr;
    if (!save_uncommitted_guard) {
      real_guards = base_->guards_;
    } else {
      real_guards = base_->uncommitted_guard_;
    }
    for (int level = 0; level < config::kNumLevels; level++) {
      const std::vector<GuardMetaData*>& base_guards = real_guards[level];
      std::vector<GuardMetaData*>::const_iterator base_iter =
          base_guards.begin();
      std::vector<GuardMetaData*>::const_iterator base_end = base_guards.end();
      const GuardSet* added_guards = levels_[level].added_guards_;
      v->guards_[level].reserve(base_guards.size() + added_guards->size());

      for (const auto& added_guard : *added_guards) {
        // Add all smaller guard listed in base_
        for (std::vector<GuardMetaData*>::const_iterator bpos =
                 std::upper_bound(base_iter, base_end, added_guard, guard_cmp);
             base_iter != bpos; ++base_iter) {
          MaybeAddGuard(v, level, *base_iter, save_uncommitted_guard);
        }

        MaybeAddGuard(v, level, added_guard, save_uncommitted_guard);
      }

      // Add remaining base files
      for (; base_iter != base_end; ++base_iter) {
        MaybeAddGuard(v, level, *base_iter, save_uncommitted_guard);
      }
      if (save_uncommitted_guard) SaveSentinelFile(v, level);
    }
  }

  void SaveFileMeta(Version* v) {
    BySmallestKey cmp;
    cmp.internal_comparator = &vset_->icmp_;
    for (int level = 0; level < config::kNumLevels; level++) {
      // Merge the set of added files with the set of pre-existing files.
      // Drop any deleted files.  Store the result in *v.
      const std::vector<FileMetaData*>& base_files = base_->files_[level];
      std::vector<FileMetaData*>::const_iterator base_iter = base_files.begin();
      std::vector<FileMetaData*>::const_iterator base_end = base_files.end();
      const FileSet* added_files = levels_[level].added_files;
      v->files_[level].reserve(base_files.size() + added_files->size());
      for (const auto& added_file : *added_files) {
        // Add all smaller files listed in base_
        for (std::vector<FileMetaData*>::const_iterator bpos =
                 std::upper_bound(base_iter, base_end, added_file, cmp);
             base_iter != bpos; ++base_iter) {
          MaybeAddFile(v, level, *base_iter);
        }

        MaybeAddFile(v, level, added_file);
      }

      // Add remaining base files
      for (; base_iter != base_end; ++base_iter) {
        MaybeAddFile(v, level, *base_iter);
      }
    }
  }

  void SaveSentinelFile(Version* v, int level) {
    const std::vector<GuardMetaData*>& guards = v->guards_[level];
    std::vector<FileMetaData*>* base_sentinel_files =
        &v->sentinel_files_[level];
    if (guards.size() == 0) {
      for (size_t i = 0; i < v->files_[level].size(); ++i) {
        base_sentinel_files->push_back(v->files_[level][i]);
      }
      return;
    }

    int guard_index = 0;
    size_t num_of_files = v->files_[level].size();

    for (size_t i = 0; i < num_of_files; ++i) {
      while (guard_index == 0 && i < num_of_files &&
             vset_->icmp_.Compare(guards[guard_index]->guard_key,
                                  v->files_[level][i]->largest) > 0) {
        base_sentinel_files->push_back(v->files_[level][i]);
        ++i;
      }
      if (guard_index == 0) {
        ++guard_index;
      }
      if (vset_->icmp_.Compare(guards[guard_index]->guard_key,
                               v->files_[level][i]->largest) >= 0) {
        if (guards[guard_index]->num_of_segments == 0) {
          guards[guard_index]->smallest_key = v->files_[level][i]->smallest;
          guards[guard_index]->level = level;
        }

        guards[guard_index]->num_of_segments++;
        guards[guard_index]->files_meta.push_back(v->files_[level][i]);
        guards[guard_index]->files_number.push_back(
            v->files_[level][i]->number);
        guards[guard_index]->largest_key = v->files_[level][i]->largest;
      } else {
        ++guard_index;
      }
    }
  }

  // Save the current state in *v.
  void SaveTo(Version* v) {
    SaveFileMeta(v);
    SaveUnCommittedGuard(v);
    SaveGuard(v);
  }

  void MaybeAddFile(Version* v, int level, FileMetaData* f) {
    if (levels_[level].deleted_files.count(f->number) > 0) {
      // File is deleted: do nothing
    } else {
      std::vector<FileMetaData*>* files = &v->files_[level];
      /*
      if (level > 0 && !files->empty()) {
        // Must not overlap
        assert(vset_->icmp_.Compare((*files)[files->size() - 1]->largest,
                                    f->smallest) < 0);
      }*/
      f->refs++;
      files->push_back(f);
    }
  }
  void MaybeAddGuard(Version* v, int level, GuardMetaData* guard,
                     bool save_uncommitted_guard) {
    std::vector<GuardMetaData*>* guards = nullptr;
    if (save_uncommitted_guard) {
      guards = &v->uncommitted_guard_[level];
    } else {
      guards = &v->guards_[level];
    }

    if (levels_[level].deleted_guards.count(guard->guard_key) > 0) {
    } else {
      if (level > 0 && !guards->empty()) {
        assert(vset_->icmp_.Compare((*guards)[guards->size() - 1]->guard_key,
                                    guard->guard_key) < 0);
      }
      guard->refs++;
      guards->push_back(guard);
    }
  }
};

VersionSet::VersionSet(const std::string& dbname, const Options* options,
                       TableCache* table_cache, BlobCache* blob_cache,
                       Cache* adaptive_cache, const InternalKeyComparator* cmp)
    : env_(options->env),
      dbname_(dbname),
      options_(options),
      table_cache_(table_cache),
      blob_cache_(blob_cache),
      adaptive_cache_(adaptive_cache),
      icmp_(*cmp),
      next_file_number_(2),
      manifest_file_number_(0),  // Filled by Recover()
      last_sequence_(0),
      log_number_(0),
      prev_log_number_(0),
      blob_number_(kInValidBlobFileNumber),
      descriptor_file_(nullptr),
      descriptor_log_(nullptr),
      dummy_versions_(this),
      current_(nullptr),
      pack_cache_and_comparator_(table_cache_, &icmp_) {
  AppendVersion(new Version(this));
}

VersionSet::~VersionSet() {
  current_->Unref();
  assert(dummy_versions_.next_ == &dummy_versions_);  // List must be empty
  delete descriptor_log_;
  delete descriptor_file_;
}

void VersionSet::AppendVersion(Version* v) {
  // Make "v" current
  assert(v->refs_ == 0);
  assert(v != current_);
  if (current_ != nullptr) {
    current_->Unref();
  }
  current_ = v;
  v->Ref();

  // Append to linked list
  v->prev_ = dummy_versions_.prev_;
  v->next_ = &dummy_versions_;
  v->prev_->next_ = v;
  v->next_->prev_ = v;
}

Status VersionSet::LogAndApply(VersionEdit* edit, port::Mutex* mu) {
  if (edit->has_log_number_) {
    assert(edit->log_number_ >= log_number_);
    assert(edit->log_number_ < next_file_number_);
  } else {
    edit->SetLogNumber(log_number_);
  }

  if (!edit->has_prev_log_number_) {
    edit->SetPrevLogNumber(prev_log_number_);
  }

  edit->SetNextFile(next_file_number_);
  edit->SetLastSequence(last_sequence_);

  Version* v = new Version(this);
  {
    Builder builder(this, current_);
    builder.Apply(edit);
    builder.SaveTo(v);
  }
  Finalize(v);

  // Initialize new descriptor log file if necessary by creating
  // a temporary file that contains a snapshot of the current version.
  std::string new_manifest_file;
  Status s;
  if (descriptor_log_ == nullptr) {
    // No reason to unlock *mu here since we only hit this path in the
    // first call to LogAndApply (when opening the database).
    assert(descriptor_file_ == nullptr);
    new_manifest_file = DescriptorFileName(dbname_, manifest_file_number_);
    edit->SetNextFile(next_file_number_);
    s = env_->NewWritableFile(new_manifest_file, &descriptor_file_);
    if (s.ok()) {
      descriptor_log_ = new log::Writer(descriptor_file_);
      s = WriteSnapshot(descriptor_log_);
    }
  }

  // Unlock during expensive MANIFEST log write
  // Note 不能释放锁否则在flush与Compaction同时进行完成后都进行LogAndApply
  // 此时版本还未升级完成,另一个抢到锁进行版本升级,导致两次都是对旧版本进行升级,会覆盖另一个进行的操作
  {
    //    mu->Unlock();

    // Write new record to MANIFEST log
    if (s.ok()) {
      std::string record;
      edit->EncodeTo(&record);
      s = descriptor_log_->AddRecord(record);
      if (s.ok()) {
        s = descriptor_file_->Sync();
      }
      if (!s.ok()) {
        Log(options_->info_log, "MANIFEST write: %s\n", s.ToString().c_str());
      }
    }

    // If we just created a new descriptor file, install it by writing a
    // new CURRENT file that points to it.
    if (s.ok() && !new_manifest_file.empty()) {
      s = SetCurrentFile(env_, dbname_, manifest_file_number_);
    }

    //    mu->Lock();
  }

  // Install the new version
  if (s.ok()) {
    AppendVersion(v);
    log_number_ = edit->log_number_;
    prev_log_number_ = edit->prev_log_number_;
  } else {
    delete v;
    if (!new_manifest_file.empty()) {
      delete descriptor_log_;
      delete descriptor_file_;
      descriptor_log_ = nullptr;
      descriptor_file_ = nullptr;
      env_->RemoveFile(new_manifest_file);
    }
  }

  return s;
}

Status VersionSet::Recover(bool* save_manifest) {
  struct LogReporter : public log::Reader::Reporter {
    Status* status;
    void Corruption(size_t bytes, const Status& s) override {
      if (this->status->ok()) *this->status = s;
    }
  };

  // Read "CURRENT" file, which contains a pointer to the current manifest
  // file
  std::string current;
  Status s = ReadFileToString(env_, CurrentFileName(dbname_), &current);
  if (!s.ok()) {
    return s;
  }
  if (current.empty() || current[current.size() - 1] != '\n') {
    return Status::Corruption("CURRENT file does not end with newline");
  }
  current.resize(current.size() - 1);

  std::string dscname = dbname_ + "/" + current;
  SequentialFile* file;
  s = env_->NewSequentialFile(dscname, &file);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      return Status::Corruption("CURRENT points to a non-existent file",
                                s.ToString());
    }
    return s;
  }

  bool have_log_number = false;
  bool have_prev_log_number = false;
  bool have_next_file = false;
  bool have_last_sequence = false;
  bool has_blob_number = false;
  uint64_t next_file = 0;
  uint64_t last_sequence = 0;
  uint64_t log_number = 0;
  uint64_t prev_log_number = 0;
  uint64_t max_blob_number = 0;

  Builder builder(this, current_);

  int read_records = 0;

  {
    LogReporter reporter;
    reporter.status = &s;
    log::Reader reader(file, &reporter, true /*checksum*/,
                       0 /*initial_offset*/);

    Slice record;
    std::string scratch;
    while (reader.ReadRecord(&record, &scratch) && s.ok()) {
      ++read_records;
      VersionEdit edit;
      s = edit.DecodeFrom(record);
      if (s.ok()) {
        if (edit.has_comparator_ &&
            edit.comparator_ != icmp_.user_comparator()->Name()) {
          s = Status::InvalidArgument(
              edit.comparator_ + " does not match existing comparator ",
              icmp_.user_comparator()->Name());
        }
      }

      if (s.ok()) {
        builder.Apply(&edit);
      }

      if (edit.has_log_number_) {
        log_number = edit.log_number_;
        have_log_number = true;
      }

      if (edit.has_prev_log_number_) {
        prev_log_number = edit.prev_log_number_;
        have_prev_log_number = true;
      }

      if (edit.has_next_file_number_) {
        next_file = edit.next_file_number_;
        have_next_file = true;
      }

      if (edit.has_last_sequence_) {
        last_sequence = edit.last_sequence_;
        have_last_sequence = true;
      }

      if (edit.has_blob_file_number_) {
        max_blob_number = edit.max_blob_number_;
        has_blob_number = true;
      }
    }
  }
  delete file;
  file = nullptr;

  if (s.ok()) {
    if (!have_next_file) {
      s = Status::Corruption("no meta-nextfile entry in descriptor");
    } else if (!have_log_number) {
      s = Status::Corruption("no meta-lognumber entry in descriptor");
    } else if (!have_last_sequence) {
      s = Status::Corruption("no last-sequence-number entry in descriptor");
    }

    if (!have_prev_log_number) {
      prev_log_number = 0;
    }

    MarkFileNumberUsed(prev_log_number);
    MarkFileNumberUsed(log_number);
  }

  if (s.ok()) {
    Version* v = new Version(this);
    builder.SaveTo(v);

    // Install recovered version
    Finalize(v);  // bug
    AppendVersion(v);
    manifest_file_number_ = next_file;
    next_file_number_ = next_file + 1;
    last_sequence_ = last_sequence;
    log_number_ = log_number;
    prev_log_number_ = prev_log_number;
    blob_number_ = max_blob_number + 1;

    // See if we can reuse the existing MANIFEST file.
    if (ReuseManifest(dscname, current)) {
      // No need to save new manifest
    } else {
      *save_manifest = true;
    }
  } else {
    std::string error = s.ToString();
    Log(options_->info_log, "Error recovering version set with %d records: %s",
        read_records, error.c_str());
  }
  return s;
}

bool VersionSet::ReuseManifest(const std::string& dscname,
                               const std::string& dscbase) {
  if (!options_->reuse_logs) {
    return false;
  }
  FileType manifest_type;
  uint64_t manifest_number;
  uint64_t manifest_size;
  if (!ParseFileName(dscbase, &manifest_number, &manifest_type) ||
      manifest_type != kDescriptorFile ||
      !env_->GetFileSize(dscname, &manifest_size).ok() ||
      // Make new compacted MANIFEST if old one is too big
      manifest_size >= TargetFileSize(options_)) {
    return false;
  }

  assert(descriptor_file_ == nullptr);
  assert(descriptor_log_ == nullptr);
  Status r = env_->NewAppendableFile(dscname, &descriptor_file_);
  if (!r.ok()) {
    Log(options_->info_log, "Reuse MANIFEST: %s\n", r.ToString().c_str());
    assert(descriptor_file_ == nullptr);
    return false;
  }

  Log(options_->info_log, "Reusing MANIFEST %s\n", dscname.c_str());
  descriptor_log_ = new log::Writer(descriptor_file_, manifest_size);
  manifest_file_number_ = manifest_number;
  return true;
}

void VersionSet::MarkFileNumberUsed(uint64_t number) {
  if (next_file_number_ <= number) {
    next_file_number_ = number + 1;
  }
}

void VersionSet::Finalize(Version* v) {
  // Precomputed best level for next compaction
  int best_level = -1;
  double best_score = -1;

  for (int level = 0; level < config::kNumLevels; level++) {
    double score = 0.0;
    double max_level_score = 0.0;
    if (level == 0) {
      // We treat level-0 specially by bounding the number of files
      // instead of number of bytes for two reasons:
      //
      // (1) With larger write-buffer sizes, it is nice not to do too
      // many level-0 compactions.
      //
      // (2) The files in level-0 are merged on every read and
      // therefore we wish to avoid too many files when the individual
      // file size is small (perhaps because of a small write-buffer
      // setting, or very high compression ratios, or lots of
      // overwrites/deletions).

      size_t sentinel_size = v->sentinel_files_[level].size();
      double sentinel_score =
          static_cast<double>(sentinel_size) / config::kL0_MaxSentinelFiles;
      v->sentinel_guard_scores_[level] = sentinel_score;
      v->compaction_scores_[level] = sentinel_score;
    } else {
      size_t sentinel_size = v->sentinel_files_[level].size();
      double sentinel_score =
          static_cast<double>(sentinel_size) / config::kMaxFilesPerGuard;
      v->sentinel_guard_scores_[level] = sentinel_score;
      max_level_score = sentinel_score;

      for (size_t i = 0; i < v->guards_[level].size(); ++i) {
        size_t num_of_files = v->guards_[level][i]->files_meta.size();
        double temp_score =
            static_cast<double>(num_of_files) / config::kMaxFilesPerGuard;
        v->guard_scores_[level].push_back(temp_score);
        max_level_score = std::max(max_level_score, temp_score);
      }
      v->compaction_scores_[level] = max_level_score;
    }
  }
}

Status VersionSet::WriteSnapshot(log::Writer* log) {
  // TODO: Break up into multiple records to reduce memory usage on
  // recovery?

  // Save metadata
  VersionEdit edit;
  edit.SetComparatorName(icmp_.user_comparator()->Name());

  // Save compaction pointers
  for (int level = 0; level < config::kNumLevels; level++) {
    if (!compact_pointer_[level].empty()) {
      InternalKey key;
      key.DecodeFrom(compact_pointer_[level]);
      edit.SetCompactPointer(level, key);
    }
  }

  // Save files
  for (int level = 0; level < config::kNumLevels; level++) {
    const std::vector<FileMetaData*>& files = current_->files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      const FileMetaData* f = files[i];
      edit.AddFile(level, f->number, f->file_size, f->smallest, f->largest);
    }
  }

  std::string record;
  edit.EncodeTo(&record);
  return log->AddRecord(record);
}

int VersionSet::NumLevelFiles(int level) const {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  return current_->files_[level].size();
}

const char* VersionSet::LevelSummary(LevelSummaryStorage* scratch) const {
  // Update code if kNumLevels changes
  static_assert(config::kNumLevels == 7, "");
  std::snprintf(
      scratch->buffer, sizeof(scratch->buffer), "files[ %d %d %d %d %d %d %d ]",
      int(current_->files_[0].size()), int(current_->files_[1].size()),
      int(current_->files_[2].size()), int(current_->files_[3].size()),
      int(current_->files_[4].size()), int(current_->files_[5].size()),
      int(current_->files_[6].size()));
  return scratch->buffer;
}

const char* VersionSet::GenerateBlobSummary(BlobSummaryStorage* scratch) const {
  scratch->buffer += "blob files[ ";

  for (const auto& blob_number : generate_blob_files_) {
    scratch->buffer += std::to_string(blob_number);
    scratch->buffer.push_back(' ');
  }
  scratch->buffer += "]";
  return scratch->buffer.c_str();
}

uint64_t VersionSet::ApproximateOffsetOf(Version* v, const InternalKey& ikey) {
  uint64_t result = 0;
  for (int level = 0; level < config::kNumLevels; level++) {
    const std::vector<FileMetaData*>& files = v->files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      if (icmp_.Compare(files[i]->largest, ikey) <= 0) {
        // Entire file is before "ikey", so just add the file size
        result += files[i]->file_size;
      } else if (icmp_.Compare(files[i]->smallest, ikey) > 0) {
        // Entire file is after "ikey", so ignore
        if (level > 0) {
          // Files other than level 0 are sorted by meta->smallest, so
          // no further files in this level will contain data for
          // "ikey".
          break;
        }
      } else {
        // "ikey" falls in the range for this table.  Add the
        // approximate offset of "ikey" within the table.
        Table* tableptr;
        Iterator* iter = table_cache_->NewIterator(
            ReadOptions(), files[i]->number, files[i]->file_size, &tableptr);
        if (tableptr != nullptr) {
          result += tableptr->ApproximateOffsetOf(ikey.Encode());
        }
        delete iter;
      }
    }
  }
  return result;
}

void VersionSet::AddLiveFiles(std::set<uint64_t>* live) {
  for (Version* v = dummy_versions_.next_; v != &dummy_versions_;
       v = v->next_) {
    for (int level = 0; level < config::kNumLevels; level++) {
      const std::vector<FileMetaData*>& files = v->files_[level];
      for (size_t i = 0; i < files.size(); i++) {
        live->insert(files[i]->number);
      }
    }
  }
}

int64_t VersionSet::NumLevelBytes(int level) const {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  return TotalFileSize(current_->files_[level]);
}

int64_t VersionSet::MaxNextLevelOverlappingBytes() {
  int64_t result = 0;
  std::vector<FileMetaData*> overlaps;
  for (int level = 1; level < config::kNumLevels - 1; level++) {
    for (size_t i = 0; i < current_->files_[level].size(); i++) {
      const FileMetaData* f = current_->files_[level][i];
      const int64_t sum = TotalFileSize(overlaps);
      if (sum > result) {
        result = sum;
      }
    }
  }
  return result;
}

// Stores the minimal range that covers all entries in inputs in
// *smallest, *largest.
// REQUIRES: inputs is not empty
void VersionSet::GetRange(const std::vector<FileMetaData*>& inputs,
                          InternalKey* smallest, InternalKey* largest) {
  assert(!inputs.empty());
  smallest->Clear();
  largest->Clear();
  for (size_t i = 0; i < inputs.size(); i++) {
    FileMetaData* f = inputs[i];
    if (i == 0) {
      *smallest = f->smallest;
      *largest = f->largest;
    } else {
      if (icmp_.Compare(f->smallest, *smallest) < 0) {
        *smallest = f->smallest;
      }
      if (icmp_.Compare(f->largest, *largest) > 0) {
        *largest = f->largest;
      }
    }
  }
}

Iterator* VersionSet::MakeInputIterator(Compaction* c) {
  ReadOptions options;
  options.verify_checksums = options_->paranoid_checks;
  options.fill_cache = false;

  // Level-0 files have to be merged together.  For other levels,
  // we will make a concatenating iterator per level.
  // TODO(opt): use concatenating iterator for level-0 if there is no
  // overlap
  const int space = (c->level() == 0 ? c->sentinel_files[0].size() + 1 : 2);
  Iterator** list = new Iterator*[space];
  int num = 0;
  for (int which = 0; which < 2; which++) {
    if (!c->sentinel_files[which].empty()) {
      if (c->level() + which == 0) {
        // Log(options_->info_log, "level-0 files %ld",
        //    c->sentinel_files[which].size());
        const std::vector<FileMetaData*>& files = c->sentinel_files[which];
        for (size_t i = 0; i < files.size(); i++) {
          list[num++] = table_cache_->NewIterator(options, files[i]->number,
                                                  files[i]->file_size);
        }
      } else {
        /*Log(options_->info_log,
            "level-%d sentinel_files size:%ld,guard size:%ld",
            c->level() + which, c->sentinel_files[which].size(),
            c->guards_inputs[which].size());*/
        const std::vector<FileMetaData*>& files = c->sentinel_files[which];
        const std::vector<GuardMetaData*>& guards = c->guards_inputs[which];
        list[num++] = NewTwoLevelIterator(
            new Version::LevelGuardNumIterator(icmp_, &files, &guards),
            &GetGuardFileIterator, &pack_cache_and_comparator_, options);
      }
    }
  }
  assert(num <= space);
  Iterator* result = NewMergingIterator(&icmp_, list, num);
  delete[] list;
  return result;
}

bool VersionSet::NeedsCompaction() const { return PickCompactionLevel() != -1; }

int VersionSet::PickCompactionLevel() const {
  for (int level = 0; level < config::kNumLevels - 2; ++level) {
    double score = current_->compaction_scores_[level];
    if (current_->compaction_scores_[level] >= 1.0 &&
        current_->compaction_scores_[level + 1] < 1.0) {
      Log(options_->info_log, "Compact Select Level-%d", level);
      return level;
    }
  }
  if (current_->compaction_scores_[config::kNumLevels - 1] >= 1.0) {
    Log(options_->info_log, "Compact Select Level-%d", config::kNumLevels - 1);
    return config::kNumLevels - 1;
  }
  return -1;
}

Compaction* VersionSet::GetInputGuardsAndFiles(
    std::vector<GuardMetaData*>* uncommitted_guards, int level) {
  int select_uncommitted_guard_level = 0;

  if (level == config::kNumLevels - 1) {  //最后一层需要进行覆写
    select_uncommitted_guard_level = level;
  } else if (level != config::kNumLevels) {  //利用下一层的guards进行分割
    select_uncommitted_guard_level = level + 1;
  }
  for (size_t i = 0;
       i < current_->uncommitted_guard_[select_uncommitted_guard_level].size();
       ++i) {
    uncommitted_guards->push_back(
        current_->uncommitted_guard_[select_uncommitted_guard_level][i]);
  }

  if (!uncommitted_guards->empty()) {
    //删除重复的guard
    GuardMetaData* prev = *uncommitted_guards->begin();
    auto iter = uncommitted_guards->begin();
    ++iter;
    while (iter != uncommitted_guards->end()) {
      if (icmp_.Compare((*iter)->guard_key.user_key(),
                        prev->guard_key.user_key()) == 0) {
        iter = uncommitted_guards->erase(iter);
      } else {
        prev = *iter;
        ++iter;
      }
    }
  }

  Compaction* c = new Compaction(options_, level);
  c->input_version_ = current_;
  c->input_version_->Ref();

  const int input_level_num = 2;

  for (int which = 0; which < input_level_num; ++which) {
    if (which + level >= config::kNumLevels) {
      continue;
    }

    std::vector<GuardMetaData*>& uncommitted_guards =
        c->input_version_->uncommitted_guard_[which + level];
    std::vector<GuardMetaData*>& guards =
        c->input_version_->guards_[which + level];
    std::vector<FileMetaData*>& sentinel_files =
        c->input_version_->sentinel_files_[which + level];

    GuardMetaData* first_uncommitted_guard = nullptr;
    if (uncommitted_guards.size() > 0) {
      first_uncommitted_guard = uncommitted_guards[0];
    }
    size_t sentinel_files_size = sentinel_files.size();
    bool add_all_sentinel_files = false;

    //挑选sentinel_files
    for (
        size_t i = 0; i < sentinel_files_size;
        ++i) {  //如果第一个uncommitted guard 与sentinel_files中一个文件有交集
                //则需要进行compaction因为sentinel_files中所有文件key必须严格小于第一个guard
      if (first_uncommitted_guard != nullptr &&
          icmp_.Compare(sentinel_files[i]->largest,
                        first_uncommitted_guard->guard_key) >= 0) {
        add_all_sentinel_files = true;
        break;
      }
    }

    //有可能该层还没有选出来uncommitted_guard 但是score已经满足compaction条件
    if (add_all_sentinel_files == false && first_uncommitted_guard == nullptr &&
        current_->compaction_scores_[level + which] >= 1.0) {
      add_all_sentinel_files = true;
    }

    //挑选guards
    size_t next_start_index = 0;
    size_t guards_size = guards.size();
    size_t uncommitted_guards_size = uncommitted_guards.size();
    std::vector<GuardMetaData*> added_all_guards;

    //此处uncommitted_guards的数量
    //必然大于等于guards的数量 guards是经过uncommitted_guards持久化得来
    for (size_t i = 0; i < uncommitted_guards_size; ++i) {
      GuardMetaData* uncommitted_guard = uncommitted_guards[i];

      GuardMetaData* first_guard = uncommitted_guards[i];
      GuardMetaData* last_guard = nullptr;
      if (i != uncommitted_guards_size - 1) {
        last_guard = uncommitted_guards[i + 1];
      }

      int guard_index = -1;
      while (next_start_index < guards_size) {
        int result = icmp_.Compare(guards[next_start_index]->guard_key,
                                   uncommitted_guard->guard_key);
        if (result == 0) {
          guard_index = result;
          ++next_start_index;
          break;
        } else if (result > 0) {
          break;
        } else {
        }
      }
      if (guard_index == -1) {
        continue;
      }

      //从挑选出来的guards中判断文件是否满足guards不相交的特性
      //不满足则需要compaction
      bool add_all_guards_file = false;

      for (size_t i = 0; i < guards[guard_index]->files_meta.size(); ++i) {
        const InternalKey smallest =
            guards[guard_index]->files_meta[i]->smallest;
        const InternalKey largest = guards[guard_index]->files_meta[i]->largest;
        //如果该guards中有文件不满足之后uncommitted_guards的范围则进行compaction
        if ((guard_index < uncommitted_guards_size - 1 &&
             (icmp_.Compare(smallest, first_guard->guard_key) < 0 ||
              icmp_.Compare(largest, last_guard->guard_key) <= 0)) ||
            (guard_index == uncommitted_guards_size - 1 &&
             icmp_.Compare(smallest, first_guard->guard_key) < 0)) {
          added_all_guards.push_back(guards[guard_index]);
          break;
        }
      }
    }
    if (add_all_sentinel_files) {
      for (size_t i = 0; i < sentinel_files_size; ++i) {
        c->sentinel_files[which].push_back(sentinel_files[i]);
        c->inputs_[which].push_back(sentinel_files[i]);
      }
    }
    for (size_t i = 0; i < added_all_guards.size(); ++i) {
      const GuardMetaData* guard = added_all_guards[i];
      for (size_t j = 0; j < guard->files_meta.size(); ++j) {
        c->inputs_[which].push_back(guard->files_meta[j]);
      }
      c->guards_inputs[which].push_back(added_all_guards[i]);
    }
  }
  return c;
}

// Finds the largest key in a vector of files. Returns true if files it not
// empty.
bool FindLargestKey(const InternalKeyComparator& icmp,
                    const std::vector<FileMetaData*>& files,
                    InternalKey* largest_key) {
  if (files.empty()) {
    return false;
  }
  *largest_key = files[0]->largest;
  for (size_t i = 1; i < files.size(); ++i) {
    FileMetaData* f = files[i];
    if (icmp.Compare(f->largest, *largest_key) > 0) {
      *largest_key = f->largest;
    }
  }
  return true;
}

// Finds minimum file b2=(l2, u2) in level file for which l2 > u1 and
// user_key(l2) = user_key(u1)
FileMetaData* FindSmallestBoundaryFile(
    const InternalKeyComparator& icmp,
    const std::vector<FileMetaData*>& level_files,
    const InternalKey& largest_key) {
  const Comparator* user_cmp = icmp.user_comparator();
  FileMetaData* smallest_boundary_file = nullptr;
  for (size_t i = 0; i < level_files.size(); ++i) {
    FileMetaData* f = level_files[i];
    if (icmp.Compare(f->smallest, largest_key) > 0 &&
        user_cmp->Compare(f->smallest.user_key(), largest_key.user_key()) ==
            0) {
      if (smallest_boundary_file == nullptr ||
          icmp.Compare(f->smallest, smallest_boundary_file->smallest) < 0) {
        smallest_boundary_file = f;
      }
    }
  }
  return smallest_boundary_file;
}

// Extracts the largest file b1 from |compaction_files| and then searches
// for a b2 in |level_files| for which user_key(u1) = user_key(l2). If it
// finds such a file b2 (known as a boundary file) it adds it to
// |compaction_files| and then searches again using this new upper bound.
//
// If there are two blocks, b1=(l1, u1) and b2=(l2, u2) and
// user_key(u1) = user_key(l2), and if we compact b1 but not b2 then a
// subsequent get operation will yield an incorrect result because it will
// return the record from b2 in level i rather than from b1 because it
// searches level by level for records matching the supplied user key.
//
// parameters:
//   in     level_files:      List of files to search for boundary files.
//   in/out compaction_files: List of files to extend by adding boundary
//   files.
void AddBoundaryInputs(const InternalKeyComparator& icmp,
                       const std::vector<FileMetaData*>& level_files,
                       std::vector<FileMetaData*>* compaction_files) {
  InternalKey largest_key;

  // Quick return if compaction_files is empty.
  if (!FindLargestKey(icmp, *compaction_files, &largest_key)) {
    return;
  }

  bool continue_searching = true;
  while (continue_searching) {
    FileMetaData* smallest_boundary_file =
        FindSmallestBoundaryFile(icmp, level_files, largest_key);

    // If a boundary file was found advance largest_key, otherwise we're
    // done.
    if (smallest_boundary_file != NULL) {
      compaction_files->push_back(smallest_boundary_file);
      largest_key = smallest_boundary_file->largest;
    } else {
      continue_searching = false;
    }
  }
}

void VersionSet::AdjustInputs(int which, Compaction* c) {
  for (size_t i = 0; i < c->sentinel_files[which].size(); ++i) {
    c->inputs_[which].emplace_back(c->sentinel_files[which][i]);
  }
  for (size_t i = 0; i < c->guards_inputs[which].size(); ++i) {
    const GuardMetaData* guard = c->guards_inputs[which][i];
    for (size_t j = 0; j < guard->files_meta.size(); ++j) {
      c->inputs_[which].emplace_back(guard->files_meta[j]);
    }
  }
}

Compaction* VersionSet::CompactRange(int level, const InternalKey* begin,
                                     const InternalKey* end) {
  std::vector<FileMetaData*> sentinel_files[2];
  std::vector<GuardMetaData*> guards[2];
  current_->GetOverlappingInputs(level, begin, end, &sentinel_files[0],
                                 &guards[0]);
  if (sentinel_files[0].size() + guards[0].size() == 0) {
    return nullptr;
  }
  current_->GetOverlappingInputs(level + 1, begin, end, &sentinel_files[1],
                                 &guards[1]);

  Compaction* c = new Compaction(options_, level);
  c->input_version_ = current_;
  c->input_version_->Ref();
  c->sentinel_files[0] = sentinel_files[0];
  c->sentinel_files[1] = sentinel_files[1];
  c->guards_inputs[0] = guards[0];
  c->guards_inputs[1] = guards[1];
  AdjustInputs(0, c);
  AdjustInputs(1, c);
  Log(options_->info_log, "level-%d sentinel_files:%ld", level,
      sentinel_files[0].size());
  Log(options_->info_log, "level-%d sentinel_files:%ld", level + 1,
      sentinel_files[1].size());
  return c;
}

Compaction::Compaction(const Options* options, int level)
    : level_(level),
      max_output_file_size_(MaxFileSizeForLevel(options, level)),
      input_version_(nullptr) {}

Compaction::~Compaction() {
  if (input_version_ != nullptr) {
    input_version_->Unref();
  }
}

void Compaction::AddInputDeletions(VersionEdit* edit) {
  for (int which = 0; which < 2; ++which) {
    for (size_t i = 0; i < sentinel_files[which].size(); ++i) {
      edit->RemoveFile(level_ + which, sentinel_files[which][i]->number);
    }
    for (size_t i = 0; i < guards_inputs[which].size(); ++i) {
      const std::vector<FileMetaData*>& guards_file =
          guards_inputs[which][i]->files_meta;
      for (size_t j = 0; j < guards_file.size(); ++j) {
        edit->RemoveFile(level_ + which, guards_file[j]->number);
      }
    }
  }
}

bool Compaction::IsBaseLevelForKey(const Slice& user_key) {
  // Maybe use binary search to find right entry instead of linear search?
  const Comparator* user_cmp = input_version_->vset_->icmp_.user_comparator();
  for (int lvl = level_ + 1; lvl < config::kNumLevels; lvl++) {
    const std::vector<FileMetaData*>& files = input_version_->files_[lvl];
    int num = FindFile(input_version_->vset_->icmp_, files, user_key, true);
    if (num < files.size()) {
      if (user_cmp->Compare(user_key, files[num]->smallest.user_key()) >= 0)
        return false;
    }
  }
  return true;
}

void Compaction::ReleaseInputs() {
  if (input_version_ != nullptr) {
    input_version_->Unref();
    input_version_ = nullptr;
  }
}

}  // namespace leveldb
