#include "util/prefetcher.h"

#include "db/memtable.h"
#include <memory>
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
  thread_pool_->Stop();
  delete iter_;  //必须手动delete
}

void Prefetcher::LazyNewIterator(const ReadOptions& options) {
  if (iter_ == nullptr) {
    iter_ = db_->NewIterator(options);
  }
}

Status Prefetcher::FetchOnlyValue(const ReadOptions& options,
                                  const Slice& start, uint32_t count,
                                  std::vector<std::string>* result) {
  return HelperFetch(options, start, count, result);
}

Status Prefetcher::FetchKV(
    const ReadOptions& options, const Slice& start, uint32_t count,
    std::vector<std::pair<std::string, std::string>>* result) {
  return HelperFetch(options, start, count, nullptr, true, result);
}

Status Prefetcher::HelperFetch(
    const ReadOptions& options, const Slice& start, uint32_t count,
    std::vector<std::string>* values, bool get_kv,
    std::vector<std::pair<std::string, std::string>>* result) {
  LazyNewIterator(options);

  int32_t i = 0;
  std::vector<std::string> temp_values;
  std::vector<std::string> temp_keys;
  if (get_kv) {
    temp_keys.reserve(count);
    temp_values.resize(count);
    values = &temp_values;
  } else {
    values->reserve(count);
  }
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
      (*values)[i] = std::move(iter_->value().ToString());
    } else {
      uint64_t offset = DecodeFixed64(iter_->value().data());
      thread_pool_->AddTask(
          std::bind(&VLog::Get, vlog_, offset, &(*values)[i]));
    }
    if (get_kv) {
      temp_keys.emplace_back(std::move(iter_->key().ToString()));
    }
    ++i;
  }

  while (!thread_pool_->AllTaskIsFinished()) {
    ;
  }

  if (get_kv) {
    assert(temp_keys.size() == temp_values.size());
    for (int i = 0; i < count; ++i) {
      result->push_back({temp_keys[i], temp_values[i]});
    }
  }

  return Status::OK();
}

Status Prefetcher::Fetch(const ReadOptions& options, const Slice& start,
                         const Slice& end, std::vector<std::string>* result,
                         bool is_forward_scan) {
  LazyNewIterator(options);

  if (start.compare(Slice()) == 0) {
    if (is_forward_scan) {
      iter_->SeekToFirst();
    } else {
      iter_->SeekToLast();
    }
  } else {
    iter_->Seek(start);
  }

  std::vector<std::future<std::string>> futures;
  std::vector<std::shared_ptr<std::promise<std::string>>> protect_promises;
  //此处利用shared_ptr保护promise不被析构

  while (iter_->Valid()) {
    bool is_memtable_iterator =
        (typeid(*iter_->current()) == typeid(MemTableIterator)) ? true : false;

    if (is_memtable_iterator) {
      result->emplace_back(iter_->value().ToString());
    } else {
      uint64_t offset = DecodeFixed64(iter_->value().data());
      std::shared_ptr<std::promise<std::string>> pro(
          std::make_shared<std::promise<std::string>>());
      protect_promises.emplace_back(pro);
      std::future<std::string> fu = pro->get_future();
      thread_pool_->AddTask(
          std::bind(&VLog::GetUsePromise, vlog_, offset, pro.get()));
      futures.emplace_back(std::move(fu));
    }

    if (iter_->key().compare(end) == 0) break;

    if (is_forward_scan) {
      iter_->Next();
    } else {
      iter_->Prev();
    }
  }

  while (!thread_pool_->AllTaskIsFinished()) {
    ;
  }

  for (auto& it : futures) {
    result->emplace_back(it.get());
  }

  return Status::OK();
}

ParseIteratorValue::ParseIteratorValue(VLog* vlog) : vlog_(vlog) {}

ParseIteratorValue::~ParseIteratorValue() {
  vlog_ = nullptr;
  Clear();
}

void ParseIteratorValue::Clear() { to_value_.clear(); }

Status ParseIteratorValue::Parse(const Slice& from_value) {
  uint64_t offset = DecodeFixed64(from_value.data());
  return vlog_->Get(offset, &to_value_);
}

std::string ParseIteratorValue::GetValue() const { return to_value_; }

}  // namespace leveldb
