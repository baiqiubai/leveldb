
#include "blob/basic_cache.h"

#include "db/filename.h"
#include <thread>
#include <typeinfo>

#include "leveldb/env.h"
#include "leveldb/table.h"

#include "util/coding.h"

#include "blob/blob.h"
namespace leveldb {

static void DeleteEntry(const Slice& key, void* value) {
  BasicAndFile* basic_file = reinterpret_cast<BasicAndFile*>(value);
  if (basic_file->is_blob_file) {
    assert(basic_file->table == nullptr);
    delete basic_file->blob;
  } else {
    assert(basic_file->blob == nullptr);
    delete basic_file->table;
  }
  delete basic_file->file;
  delete basic_file;
}

static void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

BasicAndFile::BasicAndFile(bool is_blob_file)
    : file(nullptr),
      blob(nullptr),
      table(nullptr),
      is_blob_file(is_blob_file) {}

InternalCache::InternalCache(const std::string& dbname, const Options& options,
                             int entries, bool is_blob_cache)
    : env_(options.env),
      dbname_(dbname),
      options_(options),
      cache_(NewLRUCache(entries)),
      is_blob_cache_(is_blob_cache) {}

InternalCache::~InternalCache() {
  delete cache_;
  is_blob_cache_ = false;
}

// If a seek to internal key "k" in specified file finds an entry,
// call (*handle_result)(arg, found_key, found_value).
Status InternalCache::Find(uint64_t file_number, uint64_t file_size,
                           Cache::Handle** handle) {
  Status s;
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  Slice key(buf, sizeof(buf));
  *handle = cache_->Lookup(key);
  if (*handle == nullptr) {
    std::string fname;
    Table* table = nullptr;
    Blob* blob = nullptr;
    if (is_blob_cache_) {
      fname = BlobFileName(dbname_, file_number);
    } else {
      fname = TableFileName(dbname_, file_number);
    }
    RandomAccessFile* file = nullptr;
    s = env_->NewRandomAccessFile(fname, &file);
    if (!s.ok()) {
      std::string old_fname;
      if (is_blob_cache_) {
        old_fname = BlobFileName(dbname_, file_number);
      } else {
        old_fname = SSTTableFileName(dbname_, file_number);
      }
      if (env_->NewRandomAccessFile(old_fname, &file).ok()) {
        s = Status::OK();
      }
    }
    if (s.ok()) {
      if (is_blob_cache_) {
        s = Blob::Open(options_, file, file_size, &blob);
      } else {
        s = Table::Open(options_, file, file_size, &table);
      }
    }
    if (!s.ok()) {
      assert(table == nullptr);
      assert(blob == nullptr);

      delete file;
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else {
      BasicAndFile* basic_file = new BasicAndFile(is_blob_cache_);
      if (is_blob_cache_) {
        basic_file->blob = blob;
      } else {
        basic_file->table = table;
      }
      basic_file->file = file;
      *handle = cache_->Insert(key, basic_file, 1, &DeleteEntry,
                               HandleType::kBlockHandle, 0);
    }
  }

  return s;
}

Status InternalCache::Get(const ReadOptions& options, uint64_t file_number,
                          uint64_t file_size, const Slice& k, void* arg,
                          void (*handle_result)(void*, const Slice&,
                                                const Slice&)) {
  Cache::Handle* handle = nullptr;
  // mutex_.Lock(file_number);
  Status s = Find(file_number, file_size, &handle);
  if (s.ok()) {
    Table* t = nullptr;
    Blob* b = nullptr;
    BasicAndFile* file = reinterpret_cast<BasicAndFile*>(cache_->Value(handle));
    if (is_blob_cache_) {
      b = file->blob;
      s = b->InternalGet(options, k, arg, handle_result);
    } else {
      t = file->table;
      s = t->InternalGet(options, k, arg, handle_result);
    }
    cache_->Release(handle);
  }
  // mutex_.Unlock(file_number);
  return s;
}

void InternalCache::Evict(uint64_t file_number) {
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  cache_->Erase(Slice(buf, sizeof(buf)));
}

Iterator* InternalCache::NewIterator(const ReadOptions& options,
                                     uint64_t file_number, uint64_t file_size,
                                     Table** tableptr, Blob** blobptr) {
  if (blobptr != nullptr) {
    *blobptr = nullptr;
  }
  if (tableptr != nullptr) {
    *tableptr = nullptr;
  }

  Cache::Handle* handle = nullptr;
  Status s = Find(file_number, file_size, &handle);
  if (!s.ok()) {
    return NewErrorIterator(s);
  }

  BasicAndFile* file = reinterpret_cast<BasicAndFile*>(cache_->Value(handle));
  Blob* blob = file->blob;
  Table* table = file->table;
  Iterator* result = nullptr;

  if (is_blob_cache_) {
    assert(table == nullptr);
    result = blob->NewIterator(options);
  } else {
    assert(blob == nullptr);
    result = table->NewIterator(options);
  }

  result->RegisterCleanup(&UnrefEntry, cache_, handle);

  if (blobptr != nullptr) {
    *blobptr = blob;
  }
  if (tableptr != nullptr) {
    *tableptr = table;
  }

  return result;
}

BlobCache::BlobCache(const std::string& dbname, const Options& options,
                     int entries)
    : internal_cache_(new InternalCache(dbname, options, entries, true)) {}

Iterator* BlobCache::NewIterator(const ReadOptions& options,
                                 uint64_t file_number, uint64_t file_size,
                                 Blob** blobptr) {
  return internal_cache_->NewIterator(options, file_number, file_size, nullptr,
                                      blobptr);
}

Status BlobCache::Get(const ReadOptions& options, uint64_t file_number,
                      uint64_t file_size, const Slice& k, void* arg,
                      void (*handle_result)(void*, const Slice&,
                                            const Slice&)) {
  return internal_cache_->Get(options, file_number, file_size, k, arg,
                              handle_result);
}

void BlobCache::Evict(uint64_t file_number) {
  return internal_cache_->Evict(file_number);
}

Status BlobCache::Find(uint64_t file_number, uint64_t file_size,
                       Cache::Handle** handle) {
  return internal_cache_->Find(file_number, file_size, handle);
}

TableCache::TableCache(const std::string& dbname, const Options& options,
                       int entries)
    : internal_cache_(new InternalCache(dbname, options, entries, false)) {}

Iterator* TableCache::NewIterator(const ReadOptions& options,
                                  uint64_t file_number, uint64_t file_size,
                                  Table** tableptr) {
  return internal_cache_->NewIterator(options, file_number, file_size, tableptr,
                                      nullptr);
}

Status TableCache::Get(const ReadOptions& options, uint64_t file_number,
                       uint64_t file_size, const Slice& k, void* arg,
                       void (*handle_result)(void*, const Slice&,
                                             const Slice&)) {
  return internal_cache_->Get(options, file_number, file_size, k, arg,
                              handle_result);
}

void TableCache::Evict(uint64_t file_number) {
  return internal_cache_->Evict(file_number);
}

Status TableCache::Find(uint64_t file_number, uint64_t file_size,
                        Cache::Handle** handle) {
  return internal_cache_->Find(file_number, file_size, handle);
}

}  // namespace leveldb
