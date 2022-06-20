
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
class BlobCache;

class Prefetcher {
 public:
  explicit Prefetcher(DB* db, BlobCache* blob_cache);

  ~Prefetcher();

  Status FetchInterval(const ReadOptions&, const Slice* start, const Slice* end,
                       std::vector<std::string>* result, bool is_forward_scan);

  Status FetchValue(const ReadOptions&, const Slice* start, uint32_t count,
                    std::vector<std::string>* result);

  Status FetchEntries(const ReadOptions&, const Slice* start, uint32_t count,
                      std::vector<std::pair<std::string, std::string>>* result);

 private:
  void InitIter(const ReadOptions& options, const Slice* start,
                bool is_forward_scan);

  void MoveIter(bool is_forward_scan);

  static void SaveValue(void* arg, const Slice& k, const Slice& v);

  static Status FindAndSetValue(void* arg, const ReadOptions& options,
                                uint64_t blob_number, uint64_t blob_size,
                                const Slice& blob_offset,
                                std::promise<std::string>* pro);

  Status HelpForFetch(
      const ReadOptions&, const Slice* start, uint32_t count,
      std::vector<std::string>* values, bool get_kv = false,
      std::vector<std::pair<std::string, std::string>>* result = nullptr);

  bool NeedFindInBlobCache(bool is_mem_iter, const Slice& value);

  DB* db_;
  BlobCache* blob_cache_;
  Iterator* iter_;
  std::unique_ptr<ThreadPool> thread_pool_;
};

class ParsedDBIterator {
 public:
  ParsedDBIterator(BlobCache* cache);

  Status ParseValue(uint64_t blob_number, uint64_t blob_size,
                    const Slice& decode_offset);

  std::string GetValue() const;

  ~ParsedDBIterator();

 private:
  Iterator* iter_;
  BlobCache* blob_cache_;
  std::string save_to_value_;
};

}  // namespace leveldb

#endif