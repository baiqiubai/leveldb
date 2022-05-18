
#ifndef STORAGE_LEVELDB_BLOB_PREFETCHER_H_
#define STORAGE_LEVELDB_BLOB_PREFETCHER_H_

#include <future>
#include <memory>
#include <vector>

#include "include/leveldb/status.h"

namespace leveldb {

class ReadOptions;
class Slice;
class ThreadPool;
class DB;
class Iterator;
class BasicCache;

class Prefetcher {
 public:
  explicit Prefetcher(DB* db, BasicCache* blob_cache);

  ~Prefetcher();

  Status FetchInterval(const ReadOptions&, const Slice& start, const Slice& end,
                       std::vector<std::string>* result, bool is_forward_scan);

  Status FetchValue(const ReadOptions&, const Slice& start, uint32_t count,
                    std::vector<std::string>* result);

  Status FetchEntries(const ReadOptions&, const Slice& start, uint32_t count,
                      std::vector<std::pair<std::string, std::string>>* result);

 private:
  void LazyNewIterator(const ReadOptions& options);

  struct Saver {
    std::string* value_;
  };

  static void SaveValue(void* arg, const Slice& k, const Slice& v);

  static Status PackingFunction(void* arg, const ReadOptions& options,
                                uint64_t blob_number, uint64_t blob_size,
                                const Slice& decode_blob_offset,
                                std::promise<std::string>* pro);

  Status HelpForFetch(
      const ReadOptions&, const Slice& start, uint32_t count,
      std::vector<std::string>* values, bool get_kv = false,
      std::vector<std::pair<std::string, std::string>>* result = nullptr);

  DB* db_;
  BasicCache* blob_cache_;
  Iterator* iter_;
  std::unique_ptr<ThreadPool> thread_pool_;
};

class ParseIteratorValue {
 public:
  explicit ParseIteratorValue();

  ~ParseIteratorValue();

  void Clear();

  std::string GetValue() const;

  Status Parse(const Slice& from_value);

 private:
  std::string to_value_;
};

}  // namespace leveldb

#endif