

#ifndef STORAGE_LEVELDB_BLOB_COMMON_CACHE_H_
#define STORAGE_LEVELDB_BLOB_COMMON_CACHE_H_

#include <string>

#include "leveldb/cache.h"
#include "leveldb/status.h"

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
  BasicAndFile();
  virtual ~BasicAndFile() = default;

  RandomAccessFile* file;
  bool is_blob_file;
};

struct BlobAndFile : public BasicAndFile {
  BlobAndFile() = default;
  virtual ~BlobAndFile() = default;

  Blob* blob;
};

struct TableAndFile : public BasicAndFile {
  TableAndFile() = default;
  virtual ~TableAndFile() = default;

  Table* table;
};

class BasicCache {
 public:
  BasicCache(const std::string& dbname, const Options& options, int entries);

  virtual ~BasicCache();

  // If a seek to internal key "k" in specified file finds an entry,
  // call (*handle_result)(arg, found_key, found_value).
  virtual Status Get(const ReadOptions& options, uint64_t file_number,
                     uint64_t file_size, const Slice& k, void* arg,
                     void (*handle_result)(void*, const Slice&, const Slice&));

  // Evict any entry for the specified file number
  virtual void Evict(uint64_t file_number);

  virtual Status Find(uint64_t file_number, uint64_t file_size,
                      Cache::Handle**);

  virtual Iterator* NewIterator(const ReadOptions& options,
                                uint64_t file_number, uint64_t file_size,
                                Table** tableptr = nullptr);

 protected:
  Env* const env_;
  const std::string dbname_;
  const Options& options_;
  Cache* cache_;
  bool is_blob_cache_;
};

class BlobCache : public BasicCache {
 public:
  BlobCache(const std::string& dbname, const Options& options, int entries);

  virtual ~BlobCache();
};

class TableCache : public BasicCache {
 public:
  TableCache(const std::string& dbname, const Options& options, int entries);
  virtual ~TableCache();

  // Return an iterator for the specified file number (the corresponding
  // file length must be exactly "file_size" bytes).  If "tableptr" is
  // non-null, also sets "*tableptr" to point to the Table object
  // underlying the returned iterator, or to nullptr if no Table object
  // underlies the returned iterator.  The returned "*tableptr" object is
  // owned by the cache and should not be deleted, and is valid for as long as
  // the returned iterator is live.
  Iterator* NewIterator(const ReadOptions& options, uint64_t file_number,
                        uint64_t file_size,
                        Table** tableptr = nullptr) override;

  uint64_t GetBlobNumber() const;

  uint64_t GetBlobSize() const;

 private:
  uint64_t blob_number_;
  uint64_t blob_size_;
};
}  // namespace leveldb

#endif
