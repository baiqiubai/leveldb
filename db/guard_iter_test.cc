#include "leveldb/options.h"

#include "gtest/gtest.h"
#include "version_set.h"

using namespace leveldb;

namespace TestIter {

class LevelGuardNumIterator : public Iterator {
 public:
  using Vec = std::vector<std::pair<uint64_t, uint64_t>>;

  LevelGuardNumIterator(const InternalKeyComparator& icmp,
                        const std::vector<FileMetaData*>* sentinel_files,
                        const std::vector<GuardMetaData*>* guards)
      : icmp_(icmp),
        sentinel_files_(sentinel_files),
        guards_(guards),
        guard_index_(1 + guards_->size()) {}  //置于无效位置

  bool Valid() const override { return guard_index_ < 1 + guards_->size(); }

  void SeekToFirst() override { guard_index_ = 0; }
  void SeekToLast() override {
    if (!guards_->empty()) {
      guard_index_ = guards_->size();
    } else {
      guard_index_ = 0;
    }
  }
  void Seek(const Slice& target) override {
    guard_index_ = FindGuard(icmp_, *guards_, target);
    if (guard_index_ == -1) {
      guard_index_ = 0;  //在sentinel_files中
    } else {
      guard_index_++;  //此处返回的是在guard中下标 需要越过sentinel_files
    }
    PutFileMetaInValueBuf(target);
  }
  void Next() override { guard_index_++; }
  void Prev() override { guard_index_--; }

  Slice key() const override {}
  Slice value() const override { return EncodeValueBuf(); }

  Status status() const override { return Status::OK(); }
  uint64_t GetBlobNumber() const override { return kInValidBlobFileNumber; }
  uint64_t GetBlobSize() const override { return kInValidBlobFileSize; }

  static Vec DecodeValueBuf(const Slice& buf) {
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

  void PutFileMetaInValueBuf(const Slice& target) {
    value_buf_.clear();
    result_.clear();

    if (guard_index_ == 0) {
      for (size_t i = 0; i < sentinel_files_->size(); ++i) {
        const FileMetaData* file_meta = (*sentinel_files_)[i];
        if (icmp_.Compare(file_meta->smallest.Encode(), target) <= 0 &&
            icmp_.Compare(file_meta->largest.Encode(), target) >= 0) {
          value_buf_.push_back({file_meta->number, file_meta->file_size});
        }
      }
    } else {
      if (guard_index_ > guards_->size()) {
        return;
      }
      int index = guard_index_;
      --index;
      std::vector<FileMetaData*>& files_meta = (*guards_)[index]->files_meta;
      for (size_t i = 0; i < files_meta.size(); ++i) {
        const FileMetaData* file_meta = files_meta[i];
        if (icmp_.Compare(file_meta->smallest.Encode(), target) <= 0 &&
            icmp_.Compare(file_meta->largest.Encode(), target) >= 0) {
          value_buf_.push_back({file_meta->number, file_meta->file_size});
        }
      }
    }
    if (value_buf_.size() != 0) {
      value_buf_.push_back({value_buf_.size(), 0});  //总文件数量
    }
  }

  Slice EncodeValueBuf()
      const {  // format
               // count->[file_number-file_size]->[file_number->file_size]
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

  mutable std::string result_;
  //记录着可能存在目标key的文件的file_number以及file_size
  //第一个参数为文件号第二个为文件大小 最后一个参数为总文件数量
  mutable Vec value_buf_;
};
}  // namespace TestIter

class GuardIter : public testing::Test {
 public:
  GuardIter()
      : icmp_(new InternalKeyComparator(options_.comparator)), iter_(nullptr) {}

  void AddSentinelFiles(FileMetaData* meta) { sentinel_files_.push_back(meta); }

  void AddGuards(GuardMetaData* meta) { guards_.push_back(meta); }

  ~GuardIter() {
    for (auto& file_mete : sentinel_files_) {
      delete file_mete;
    }
    for (auto& guard_meta : guards_) {
      delete guard_meta;
    }
    sentinel_files_.clear();
    guards_.clear();
    delete icmp_;
    delete iter_;
  }

  void NewIter() {
    iter_ =
        new TestIter::LevelGuardNumIterator(*icmp_, &sentinel_files_, &guards_);
  }

  void SeekToFirst() {
    assert(iter_);
    iter_->SeekToFirst();
  }

  void SeekToLast() {
    assert(iter_);
    iter_->SeekToLast();
  }

  void Seek(const Slice& internal_key) {
    assert(iter_);
    iter_->Seek(internal_key);
  }
  void Next() {
    assert(iter_);
    iter_->Next();
  }

  void Prev() {
    assert(iter_);
    iter_->Prev();
  }

  int GuardIndex() const {
    assert(iter_);
    return iter_->GuardIndex();
  }
  bool Valid() const { return iter_->Valid(); }

  Slice value() const { return iter_->value(); }

 private:
  Options options_;
  const InternalKeyComparator* icmp_;
  std::vector<FileMetaData*> sentinel_files_;
  std::vector<GuardMetaData*> guards_;

  TestIter::LevelGuardNumIterator* iter_;
};

FileMetaData* GetFileMeta(const char* smallest, const char* largest,
                          SequenceNumber smallest_seq = 100,
                          SequenceNumber largest_seq = 100,
                          uint64_t file_number = 0) {
  FileMetaData* f = new FileMetaData();
  f->number = file_number;
  f->file_size = 0;
  f->smallest = InternalKey(smallest, smallest_seq, kTypeValue);
  f->largest = InternalKey(largest, largest_seq, kTypeValue);
  return f;
}

GuardMetaData* GetGuardMeta(const InternalKey& smallest,
                            const InternalKey& largest,
                            const InternalKey& guard_key) {
  GuardMetaData* guard = new GuardMetaData();
  guard->guard_key = guard_key;
  guard->smallest_key = smallest;
  guard->largest_key = largest;
  return guard;
}

void AddFileToGuard(GuardMetaData* guard, FileMetaData* file) {
  guard->files_meta.emplace_back(file);
  guard->files_number.emplace_back(file->number);
}

TEST_F(GuardIter, GuardEmpty) {
  AddSentinelFiles(GetFileMeta("a", "b"));
  NewIter();
  ASSERT_EQ(Valid(), false);
  ASSERT_EQ(GuardIndex(), 1);
  SeekToFirst();
  ASSERT_EQ(Valid(), true);
  ASSERT_EQ(GuardIndex(), 0);
  SeekToLast();
  ASSERT_EQ(Valid(), true);
  ASSERT_EQ(GuardIndex(), 0);
  Seek(InternalKey("a", 100, kTypeValue).Encode());
  ASSERT_EQ(Valid(), true);
  ASSERT_EQ(GuardIndex(), 0);
  TestIter::LevelGuardNumIterator::Vec vec =
      TestIter::LevelGuardNumIterator::DecodeValueBuf(value());
  for (auto& [file_number, file_size] : vec) {
    ASSERT_EQ(file_number, 0);
    ASSERT_EQ(file_size, 0);
  }
}

TEST_F(GuardIter, OneGuard) {
  AddSentinelFiles(GetFileMeta("a", "c", 100, 100, 1));
  AddSentinelFiles(GetFileMeta("a", "b", 99, 99, 2));
  GuardMetaData* guard = GetGuardMeta(InternalKey("c", 99, kTypeValue),
                                      InternalKey("e", 99, kTypeValue),
                                      InternalKey("c", 99, kTypeValue));
  AddFileToGuard(guard, GetFileMeta("c", "e", 99, 99, 3));
  AddGuards(guard);

  NewIter();
  ASSERT_EQ(Valid(), false);
  ASSERT_EQ(GuardIndex(), 2);
  SeekToFirst();
  ASSERT_EQ(Valid(), true);
  ASSERT_EQ(GuardIndex(), 0);
  Next();
  ASSERT_EQ(Valid(), true);
  ASSERT_EQ(GuardIndex(), 1);
  Prev();
  ASSERT_EQ(Valid(), true);
  ASSERT_EQ(GuardIndex(), 0);
  Seek(InternalKey("b", 100, kTypeValue).Encode());
  TestIter::LevelGuardNumIterator::Vec vec =
      TestIter::LevelGuardNumIterator::DecodeValueBuf(value());
  ASSERT_EQ(vec.size(), 2);
  ASSERT_EQ(Valid(), true);
  ASSERT_EQ(GuardIndex(), 0);
  Seek(InternalKey("a", 101, kTypeValue).Encode());
  vec = TestIter::LevelGuardNumIterator::DecodeValueBuf(value());
  ASSERT_EQ(vec.size(), 0);
  Seek(InternalKey("c", 99, kTypeValue).Encode());
  ASSERT_EQ(Valid(), true);
  ASSERT_EQ(GuardIndex(), 1);
  vec = TestIter::LevelGuardNumIterator::DecodeValueBuf(value());
  ASSERT_EQ(vec.size(), 1);
  Seek(InternalKey("e", 98, kTypeValue).Encode());
  ASSERT_EQ(GuardIndex(), 1);
  vec = TestIter::LevelGuardNumIterator::DecodeValueBuf(value());
  ASSERT_EQ(vec.size(), 0);
}

TEST_F(GuardIter, TwoGuard) {
  AddSentinelFiles(GetFileMeta("a", "c", 100, 100, 1));
  GuardMetaData* guard1 = GetGuardMeta(InternalKey("c", 99, kTypeValue),
                                       InternalKey("e", 99, kTypeValue),
                                       InternalKey("c", 99, kTypeValue));
  // guard1包含两个file
  AddFileToGuard(guard1, GetFileMeta("c", "d", 99, 100, 3));
  AddFileToGuard(guard1, GetFileMeta("d", "e", 99, 99, 4));

  GuardMetaData* guard2 = GetGuardMeta(InternalKey("e", 97, kTypeValue),
                                       InternalKey("z", 100, kTypeValue),
                                       InternalKey("e", 97, kTypeValue));
  AddFileToGuard(guard2, GetFileMeta("e", "z", 97, 100, kTypeValue));
  AddGuards(guard1);
  AddGuards(guard2);
  NewIter();

  ASSERT_EQ(Valid(), false);
  ASSERT_EQ(GuardIndex(), 3);

  Seek(InternalKey("e", 99, kTypeValue).Encode());
  ASSERT_EQ(GuardIndex(), 1);
  TestIter::LevelGuardNumIterator::Vec vec =
      TestIter::LevelGuardNumIterator::DecodeValueBuf(value());
  ASSERT_EQ(vec.size(), 1);

  Seek(InternalKey("e", 96, kTypeValue).Encode());
  ASSERT_EQ(Valid(), true);
  ASSERT_EQ(GuardIndex(), 2);
  vec = TestIter::LevelGuardNumIterator::DecodeValueBuf(
      value());  //实际不存在因此vec为0
  ASSERT_EQ(vec.size(), 1);
  Seek(InternalKey("e", 97, kTypeValue).Encode());
  ASSERT_EQ(Valid(), true);
  ASSERT_EQ(GuardIndex(), 2);
  vec = TestIter::LevelGuardNumIterator::DecodeValueBuf(value());
  ASSERT_EQ(vec.size(), 1);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
