
#ifndef STORAGE_LEVELDB_BLOB_BLOB_H_
#define STORAGE_LEVELDB_BLOB_BLOB_H_

#include "leveldb/iterator.h"
#include "leveldb/options.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"

namespace leveldb {

class RandomAccessFile;
class Iterator;
class BlockContents;

class BlobBlock {
 public:
  BlobBlock(const BlockContents& contents);

  BlobBlock(const BlobBlock&) = delete;
  BlobBlock& operator=(const BlobBlock&) = delete;

  Iterator* NewIterator(const ReadOptions& options) const;

  ~BlobBlock();

  size_t size() const;

  class Iter;

 private:
  const char* data_;
  size_t size_;
  bool owner_;
};

class Blob {
 public:
  static Status Open(const Options& options, RandomAccessFile* file,
                     uint64_t file_size, Blob** blobptr);

  Status InternalGet(const ReadOptions& options, const Slice& k, void* arg,
                     void (*handle_result)(void*, const Slice&, const Slice&));
  ~Blob();

  Iterator* NewIterator(const ReadOptions& options);

 private:
  Iterator* BlockReader(void* arg, const ReadOptions& options,
                        uint64_t handle_offset);

  class Rep;
  explicit Blob(Rep* rep) : rep_(rep) {}
  Rep* rep_;
};
}  // namespace leveldb
#endif