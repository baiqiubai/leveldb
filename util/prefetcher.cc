#include "util/prefetcher.h"

#include "db/memtable.h"
#include <typeinfo>

#include "table/vlog_format.h"
#include "util/coding.h"
#include "util/threadpool.h"

#include "include/leveldb/db.h"
#include "include/leveldb/slice.h"

namespace leveldb {

Prefetcher::Prefetcher(DB* db, VLog* vlog)
    : db_(db),
      vlog_(vlog),
      iter_(nullptr),
      thread_pool_(new ThreadPool(2 * ThreadConfig::GetCPUCore())) {
  thread_pool_->Start();
}

Prefetcher::~Prefetcher() {
  // thread_pool_->Stop();
  delete iter_;  //必须手动delete
}

void Prefetcher::LazyNewIterator(const ReadOptions& options) {
  if (iter_ == nullptr) {
    iter_ = db_->NewIterator(options);
  }
}

Status Prefetcher::Fetch(const ReadOptions& options, const Slice& start,
                         const Slice& end, std::vector<std::string>* result) {
  LazyNewIterator(options);

  if (start.compare(Slice()) == 0) {
    iter_->SeekToFirst();
  } else {
    iter_->Seek(start);
  }

  std::vector<std::future<std::string>> futures;

  while (iter_->Valid()) {
    bool is_memtable_iterator =
        (typeid(*iter_->current()) == typeid(MemTableIterator)) ? true : false;

    if (is_memtable_iterator) {
      result->emplace_back(iter_->value().ToString());
    } else {
      uint64_t offset = DecodeFixed64(iter_->value().data());
      std::promise<std::string> pro;
      std::future<std::string> fu = pro.get_future();
      thread_pool_->AddTask(
          std::bind(&VLog::GetUsePromise, vlog_, offset, &pro));
      futures.emplace_back(std::move(fu));
    }

    if (iter_->key().compare(end) == 0) break;

    iter_->Next();
  }

  for (auto& it : futures) {
    result->emplace_back(it.get());
  }

  while (!thread_pool_->AllTaskIsFinished()) {
    ;
  }

  thread_pool_->Stop();

  return Status::OK();
}

Status Prefetcher::Fetch(const ReadOptions& options, const Slice& start,
                         uint32_t count, std::vector<std::string>* result) {
  LazyNewIterator(options);

  int32_t i = 0;
  result->resize(count);
  thread_pool_->SetTaskNum(count);

  if (start.compare(Slice()) == 0) {
    iter_->SeekToFirst();
  } else {
    iter_->Seek(start);
  }

  for (; iter_->Valid() && count--; iter_->Next()) {
    bool is_memtable_iterator =
        (typeid(*iter_->current()) == typeid(MemTableIterator)) ? true : false;
    if (is_memtable_iterator) {
      (*result)[i] = iter_->value().ToString();
    } else {
      uint64_t offset = DecodeFixed64(iter_->value().data());
      thread_pool_->AddTask(
          std::bind(&VLog::Get, vlog_, offset, &(*result)[i]));
    }
    ++i;
  }

  while (thread_pool_->GetRemainTaskNum()) {
    ;
  }
  return Status::OK();
}

ParseIteratorValue::ParseIteratorValue(VLog* vlog) : vlog_(vlog) {}

ParseIteratorValue::~ParseIteratorValue() {
  vlog_ = nullptr;
  to_value_.clear();
}

Status ParseIteratorValue::Parse(const Slice& from_value) {
  uint64_t offset = DecodeFixed64(from_value.data());
  return vlog_->Get(offset, &to_value_);
}

std::string ParseIteratorValue::GetValue() const { return to_value_; }

}  // namespace leveldb
