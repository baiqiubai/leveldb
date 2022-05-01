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

Status Prefetcher::Fetch(const ReadOptions& options, const Slice& start,
                         uint32_t count, std::vector<std::string>* result) {
  if (iter_ == nullptr) {
    iter_ = db_->NewIterator(options);
  }

  result->resize(count);
  thread_pool_->SetTaskNum(count);
  int32_t i = 0;

  iter_->Seek(start);
  for (; iter_->Valid() && count--; iter_->Next()) {
    if (typeid(*iter_->current()) ==
        typeid(MemTableIterator)) {  //运行期识别 如果是memtable则没有kv分离
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
  thread_pool_->Stop();
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
