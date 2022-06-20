#include "blob/prefetcher.h"

#include "db/memtable.h"
#include <memory>
#include <typeinfo>

#include "leveldb/db.h"
#include "leveldb/slice.h"

#include "util/coding.h"

#include "blob/basic_cache.h"
#include "blob/threadpool.h"

namespace leveldb {

Prefetcher::Prefetcher(DB* db, BlobCache* blob_cache)
    : db_(db),
      blob_cache_(blob_cache),
      iter_(nullptr),
      thread_pool_(new ThreadPool(2 * ThreadConfig::GetCPUCore())) {
  thread_pool_->Start();
}

Prefetcher::~Prefetcher() {
  thread_pool_->Stop();
  delete iter_;
}

Status Prefetcher::FetchValue(const ReadOptions& options, const Slice* start,
                              uint32_t count,
                              std::vector<std::string>* result) {
  return HelpForFetch(options, start, count, result);
}

Status Prefetcher::FetchEntries(
    const ReadOptions& options, const Slice* start, uint32_t count,
    std::vector<std::pair<std::string, std::string>>* result) {
  return HelpForFetch(options, start, count, nullptr, true, result);
}

void Prefetcher::SaveValue(void* arg, const Slice& k, const Slice& v) {
  std::string* result = reinterpret_cast<std::string*>(arg);
  result->assign(v.data(), v.size());
}

Status Prefetcher::FindAndSetValue(void* arg, const ReadOptions& options,
                                   uint64_t blob_number, uint64_t blob_size,
                                   const Slice& blob_offset,
                                   std::promise<std::string>* pro) {
  assert(blob_offset.data()[0] == KVSeparation::kSeparation);
  assert(blob_offset.size() == 9);

  BlobCache* blob_cache = reinterpret_cast<BlobCache*>(arg);
  std::string value;

  Status s = blob_cache->Get(options, blob_number, blob_size, value, &value,
                             &SaveValue);
  if (s.ok()) {
    pro->set_value(value);
  }
  return s;
}

Status Prefetcher::HelpForFetch(
    const ReadOptions& options, const Slice* start, uint32_t count,
    std::vector<std::string>* values, bool get_kv,
    std::vector<std::pair<std::string, std::string>>* result) {
  InitIter(options, start, true);

  if (!iter_->Valid()) {
    return iter_->status();
  }

  int32_t i = 0;
  uint32_t num_of_entries = count;
  std::vector<std::pair<std::string, std::string>> temp;
  temp.resize(num_of_entries);

  while (iter_->Valid() && count--) {
    temp[i].first = iter_->key().ToString();

    Slice value = iter_->value();
    if (NeedFindInBlobCache(iter_->IsMemIter(), value)) {
      thread_pool_->AddTask(std::bind(
          &BlobCache::Get, blob_cache_, options, iter_->GetBlobNumber(),
          iter_->GetBlobSize(), iter_->value(), &temp[i].second, &SaveValue));
    } else {
      if (iter_->IsMemIter()) {
        temp[i].second = value.ToString();
      } else {
        assert(value.data()[0] == kNoSeparation);
        temp[i].second = Slice(value.data() + 1, value.size() - 1).ToString();
      }
    }
    MoveIter(true);
    ++i;
  }

  while (!thread_pool_->AllTaskIsFinished()) {
    ;
  }

  if (!get_kv) {
    values->resize(num_of_entries);
    for (uint32_t i = 0; i < num_of_entries; ++i) {
      (*values)[i] = std::move(temp[i].second);
    }
  } else {
    *result = std::vector<std::pair<std::string, std::string>>(temp.begin(),
                                                               temp.end());
  }

  return Status::OK();
}

void Prefetcher::InitIter(const ReadOptions& options, const Slice* start,
                          bool is_forward_scan) {
  delete iter_;
  iter_ = db_->NewIterator(options);

  if (start == nullptr) {
    if (is_forward_scan) {
      iter_->SeekToFirst();
    } else {
      iter_->SeekToLast();
    }
  } else {
    iter_->Seek(*start);
  }
}

void Prefetcher::MoveIter(bool is_forward_scan) {
  if (is_forward_scan) {
    iter_->Next();
  } else {
    iter_->Prev();
  }
}

bool Prefetcher::NeedFindInBlobCache(bool is_mem_iter, const Slice& value) {
  return !is_mem_iter && value.data()[0] == kSeparation;
}

Status Prefetcher::FetchInterval(const ReadOptions& options, const Slice* start,
                                 const Slice* end,
                                 std::vector<std::string>* result,
                                 bool is_forward_scan) {
  InitIter(options, start, is_forward_scan);

  if (!iter_->Valid()) {
    return iter_->status();
  }

  std::vector<std::future<std::string>> futures;
  std::vector<std::shared_ptr<std::promise<std::string>>> protect_promises;
  //此处利用shared_ptr保护promise不被析构

  while (iter_->Valid()) {
    std::shared_ptr<std::promise<std::string>> pro(
        std::make_shared<std::promise<std::string>>());
    std::future<std::string> fu = pro->get_future();
    Slice value = iter_->value();
    protect_promises.emplace_back(pro);

    if (NeedFindInBlobCache(iter_->IsMemIter(), value)) {
      thread_pool_->AddTask(std::bind(&Prefetcher::FindAndSetValue,
                                      reinterpret_cast<void*>(blob_cache_),
                                      options, iter_->GetBlobNumber(),
                                      iter_->GetBlobSize(), value, pro.get()));
    } else {
      if (iter_->IsMemIter()) {
        pro->set_value(value.ToString());
      } else {
        assert(value.data()[0] == kNoSeparation);
        pro->set_value(Slice(value.data() + 1, value.size() - 1).ToString());
      }
    }
    futures.emplace_back(std::move(fu));

    if (end != nullptr && iter_->key().compare(*end) == 0) {
      break;
    }

    MoveIter(is_forward_scan);
  }

  while (!thread_pool_->AllTaskIsFinished()) {
    ;
  }

  for (auto& it : futures) {
    result->emplace_back(it.get());
  }

  return Status::OK();
}

ParsedDBIterator::ParsedDBIterator(BlobCache* cache) : blob_cache_(cache) {}

Status ParsedDBIterator::ParseValue(uint64_t blob_number, uint64_t blob_size,
                                    const Slice& decode_offset) {
  Saver saver;
  saver.value = &save_to_value_;
  return blob_cache_->Get(ReadOptions(), blob_number, blob_size, decode_offset,
                          &saver, SaveValue);
}

std::string ParsedDBIterator::GetValue() const { return save_to_value_; }

ParsedDBIterator::~ParsedDBIterator() { save_to_value_.clear(); }

}  // namespace leveldb
