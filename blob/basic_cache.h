

#ifndef STORAGE_LEVELDB_BLOB_COMMON_CACHE_H_
#define STORAGE_LEVELDB_BLOB_COMMON_CACHE_H_

#include "db/dbformat.h"
#include <memory>
#include <mutex>
#include <string>

#include "leveldb/cache.h"
#include "leveldb/status.h"

#include "util/hash.h"

namespace leveldb {

class Env;
class Options;
class Cache;
class ReadOptions;
class Slice;
class RandomAccessFile;
class Table;
class Iterator;
class Blob;
class Table;

struct BasicAndFile {
  BasicAndFile(bool is_blob_file);
  ~BasicAndFile() = default;

  RandomAccessFile* file;
  Blob* blob;
  Table* table;
  bool is_blob_file;
};

class SharedMutex {
 public:
  void Lock(uint64_t file_number) {
    shared[Shard(static_cast<uint32_t>(file_number))].lock();
  }
  void Unlock(uint64_t file_number) {
    shared[Shard(static_cast<uint32_t>(file_number))].unlock();
  }

 private:
  static inline uint32_t HashSlice(const Slice& s) {
    return Hash(s.data(), s.size(), 0);
  }

  static uint32_t Shard(uint32_t hash) { return hash >> (32 - kNumShardBits); }

 public:
  std::mutex shared[kNumShards];
  int capacity_;
};

class InternalCache {
 public:
  InternalCache(const std::string& dbname, const Options& options, int entries,
                bool is_blob_cache = false);

  ~InternalCache();

  InternalCache(const InternalCache&) = delete;
  InternalCache& operator=(const InternalCache&) = delete;

  // If a seek to internal key "k" in specified file finds an entry,
  // call (*handle_result)(arg, found_key, found_value).
  Status Get(const ReadOptions& options, uint64_t file_number,
             uint64_t file_size, const Slice& k, void* arg,
             void (*handle_result)(void*, const Slice&, const Slice&));

  Status Find(uint64_t file_number, uint64_t file_size, Cache::Handle**);

  Iterator* NewIterator(const ReadOptions& options, uint64_t file_number,
                        uint64_t file_size, Table** tableptr = nullptr,
                        Blob** blobptr = nullptr);

  void Evict(uint64_t file_number);

 private:
  Env* const env_;
  const std::string dbname_;
  const Options& options_;
  Cache* cache_;
  bool is_blob_cache_;
};

class BlobCache {
 public:
  BlobCache(const std::string& dbname, const Options& options, int entries);

  ~BlobCache() = default;

  Iterator* NewIterator(const ReadOptions& options, uint64_t file_number,
                        uint64_t file_size, Blob** blobptr = nullptr);

  // If a seek to internal key "k" in specified file finds an entry,
  // call (*handle_result)(arg, found_key, found_value).
  Status Get(const ReadOptions& options, uint64_t file_number,
             uint64_t file_size, const Slice& k, void* arg,
             void (*handle_result)(void*, const Slice&, const Slice&));

  // Evict any entry for the specified file number
  void Evict(uint64_t file_number);

  Status Find(uint64_t file_number, uint64_t file_size, Cache::Handle**);

 private:
  std::unique_ptr<InternalCache> internal_cache_;
};

class TableCache {
 public:
  TableCache(const std::string& dbname, const Options& options, int entries);
  ~TableCache() = default;

  // Return an iterator for the specified file number (the corresponding
  // file length must be exactly "file_size" bytes).  If "tableptr" is
  // non-null, also sets "*tableptr" to point to the Table object
  // underlying the returned iterator, or to nullptr if no Table object
  // underlies the returned iterator.  The returned "*tableptr" object is
  // owned by the cache and should not be deleted, and is valid for as long as
  // the returned iterator is live.
  Iterator* NewIterator(const ReadOptions& options, uint64_t file_number,
                        uint64_t file_size, Table** tableptr = nullptr);

  // If a seek to internal key "k" in specified file finds an entry,
  // call (*handle_result)(arg, found_key, found_value).
  Status Get(const ReadOptions& options, uint64_t file_number,
             uint64_t file_size, const Slice& k, void* arg,
             void (*handle_result)(void*, const Slice&, const Slice&));

  // Evict any entry for the specified file number
  void Evict(uint64_t file_number);

  Status Find(uint64_t file_number, uint64_t file_size, Cache::Handle**);

 private:
  std::unique_ptr<InternalCache> internal_cache_;
};

}  // namespace leveldb

#endif
