
#include "blob/basic_cache.h"

#include "db/filename.h"
#include <typeinfo>

#include "leveldb/env.h"
#include "leveldb/table.h"

#include "util/coding.h"

#include "blob/blob.h"
namespace leveldb {

static void DeleteEntry(const Slice& key, void* value) {
  BasicAndFile* basic_file = reinterpret_cast<BasicAndFile*>(value);
  if (basic_file->is_blob_file) {
    BlobAndFile* bf = dynamic_cast<BlobAndFile*>(basic_file);
    delete bf->blob;
    delete bf->file;
  } else {
    TableAndFile* tf = dynamic_cast<TableAndFile*>(basic_file);
    delete tf->table;
    delete tf->file;
  }
  delete basic_file;
}

static void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

BasicAndFile::BasicAndFile() : file(nullptr), is_blob_file(false) {
  if (typeid(this) == typeid(BlobAndFile)) {
    is_blob_file = true;
  }
}

BasicCache::BasicCache(const std::string& dbname, const Options& options,
                       int entries)

    : env_(options.env),
      dbname_(dbname),
      options_(options),
      cache_(NewLRUCache(entries)),
      is_blob_cache_(false) {
  if (typeid(this) == typeid(BlobCache)) {
    is_blob_cache_ = true;
  }
}

BasicCache::~BasicCache() { delete cache_; }

Iterator* BasicCache::NewIterator(const ReadOptions& options,
                                  uint64_t file_number, uint64_t file_size,
                                  Table** tableptr) {
  return nullptr;
}

// If a seek to internal key "k" in specified file finds an entry,
// call (*handle_result)(arg, found_key, found_value).
Status BasicCache::Find(uint64_t file_number, uint64_t file_size,
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
    if (is_blob_cache_ == false) {
      fname = TableFileName(dbname_, file_number);
    } else {
      fname = BlobFileName(dbname_, file_number);
    }
    RandomAccessFile* file = nullptr;
    s = env_->NewRandomAccessFile(fname, &file);
    if (!s.ok()) {
      std::string old_fname;
      if (is_blob_cache_) {
        old_fname = SSTTableFileName(dbname_, file_number);
      } else {
        old_fname = BlobFileName(dbname_, file_number);
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
      BasicAndFile* basic_file = nullptr;
      if (is_blob_cache_) {
        basic_file = new BlobAndFile;
        dynamic_cast<BlobAndFile*>(basic_file)->blob = blob;
      } else {
        basic_file = new TableAndFile;
        dynamic_cast<TableAndFile*>(basic_file)->table = table;
      }
      basic_file->file = file;
      *handle = cache_->Insert(key, basic_file, 1, &DeleteEntry);
    }
  }
  return s;
}

Status BasicCache::Get(const ReadOptions& options, uint64_t file_number,
                       uint64_t file_size, const Slice& k, void* arg,
                       void (*handle_result)(void*, const Slice&,
                                             const Slice&)) {
  Cache::Handle* handle = nullptr;
  Status s = Find(file_number, file_size, &handle);
  if (s.ok()) {
    Table* t = nullptr;
    Blob* b = nullptr;
    if (is_blob_cache_) {
      b = reinterpret_cast<BlobAndFile*>(cache_->Value(handle))->blob;
      s = b->InternalGet(options, k, arg, handle_result);
    } else {
      t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
      s = t->InternalGet(options, k, arg, handle_result);
    }
    cache_->Release(handle);
  }
  return s;
}

void BasicCache::Evict(uint64_t file_number) {
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  cache_->Erase(Slice(buf, sizeof(buf)));
}

BlobCache::BlobCache(const std::string& dbname, const Options& options,
                     int entries)
    : BasicCache(dbname, options, entries) {}

BlobCache::~BlobCache() {}

TableCache::TableCache(const std::string& dbname, const Options& options,
                       int entries)
    : BasicCache(dbname, options, entries) {}

TableCache::~TableCache() {}

uint64_t TableCache::GetBlobNumber() const { return blob_number_; }

uint64_t TableCache::GetBlobSize() const { return blob_size_; }

Iterator* TableCache::NewIterator(const ReadOptions& options,
                                  uint64_t file_number, uint64_t file_size,
                                  Table** tableptr) {
  if (tableptr != nullptr) {
    *tableptr = nullptr;
  }

  Cache::Handle* handle = nullptr;
  Status s = Find(file_number, file_size, &handle);
  if (!s.ok()) {
    return NewErrorIterator(s);
  }

  Table* table = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
  blob_number_ = table->GetBlobNumber();
  blob_size_ = table->GetBlobSize();
  Iterator* result = table->NewIterator(options);
  result->RegisterCleanup(&UnrefEntry, cache_, handle);
  if (tableptr != nullptr) {
    *tableptr = table;
  }
  return result;
}

}  // namespace leveldb
