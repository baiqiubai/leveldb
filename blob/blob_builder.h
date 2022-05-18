
#ifndef STORAGE_LEVELDB_BLOB_BLOB_BUILDER_H_
#define STORAGE_LEVELDB_BLOB_BLOB_BUILDER_H_

#include <string>

#include "table/format.h"

#include "include/leveldb/options.h"
#include "include/leveldb/status.h"

namespace leveldb {

class Slice;
class WritableFile;

class BlobBuilder {
 public:
  explicit BlobBuilder(const Options& options, WritableFile* file);

  ~BlobBuilder();

  BlobBuilder(const BlobBuilder&) = delete;
  BlobBuilder& operator=(const BlobBuilder&) = delete;

  void Add(const Slice& key, const Slice& value);

  Status Finish();

  uint64_t CurrentSizeEstimate() const;

  uint64_t FileSize() const;

 private:
  std::string DecodeEntry(const Slice& key, const Slice& value);

  Status WriteFooter();

  const Options options_;
  WritableFile* file_;
  std::string buffer_;
  uint64_t offset_;
  BlockHandle pending_handle_;
};

};  // namespace leveldb

#endif