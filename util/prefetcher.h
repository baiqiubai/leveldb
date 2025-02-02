
#ifndef STORAGE_LEVELDB_UTIL_PREFETCHER_H_
#define STORAGE_LEVELDB_UTIL_PREFETCHER_H_

#include <memory>
#include <vector>

#include "include/leveldb/status.h"

namespace leveldb {

class ReadOptions;
class Slice;
class VLog;
class ThreadPool;
class DB;
class Iterator;

class Prefetcher {
 public:
  explicit Prefetcher(DB* db, VLog* vlog);

  ~Prefetcher();

  Status Fetch(const ReadOptions&, const Slice& start, const Slice& end,
               std::vector<std::string>* result, bool is_forward_scan);

  Status FetchOnlyValue(const ReadOptions&, const Slice& start, uint32_t count,
                        std::vector<std::string>* result);

  Status FetchKV(const ReadOptions&, const Slice& start, uint32_t count,
                 std::vector<std::pair<std::string, std::string>>* result);

 private:
  void LazyNewIterator(const ReadOptions& options);

  Status HelperFetch(
      const ReadOptions&, const Slice& start, uint32_t count,
      std::vector<std::string>* values, bool get_kv = false,
      std::vector<std::pair<std::string, std::string>>* result = nullptr);

  DB* db_;
  VLog* vlog_;
  Iterator* iter_;
  std::unique_ptr<ThreadPool> thread_pool_;
};

class ParseIteratorValue {
 public:
  explicit ParseIteratorValue(VLog* vlog);

  ~ParseIteratorValue();

  void Clear();

  std::string GetValue() const;

  Status Parse(const Slice& from_value);

 private:
  VLog* vlog_;
  std::string to_value_;
};

}  // namespace leveldb

#endif