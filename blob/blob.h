
#ifndef STORAGE_LEVELDB_BLOB_BLOB_H_
#define STORAGE_LEVELDB_BLOB_BLOB_H_

#include "leveldb/options.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"

namespace leveldb {

class RandomAccessFile;

class Blob {
 public:
  static Status Open(const Options& options, RandomAccessFile* file,
                     uint64_t file_size, Blob** blobptr);

  Status InternalGet(const ReadOptions& options, const Slice& k, void* arg,
                     void (*handle_result)(void*, const Slice&, const Slice&));
  ~Blob();

 private:
  Status BlockReader(void* arg, const ReadOptions& options,
                     uint64_t handle_offset);

  Slice DecodeEntry(uint64_t offset);
  class Rep;
  explicit Blob(Rep* rep) : rep_(rep) {}
  Rep* rep_;
};
}  // namespace leveldb
#endif