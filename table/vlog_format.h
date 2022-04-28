

#ifndef STORAGE_LEVELDB_TABLE_VLOG_FORMAT_H_
#define STORAGE_LEVELDB_TABLE_VLOG_FORMAT_H_

#include <memory>

#include "include/leveldb/status.h"

namespace leveldb {

class DB;
class Options;
class Env;
class Slice;
class WritableFile;
class RandomAccessFile;
class RandomWriteFile;
class SequentialFile;

class VLog {
 public:
  explicit VLog(DB* db, const Options* options, Env* env,
                const std::string& vlog_name);

  VLog(const VLog&) = delete;
  VLog& operator=(const VLog&) = delete;

  void Add(const Slice& key, const Slice& value);

  Status Get(uint64_t offset, std::string* value);

  uint64_t Tail() const { return tail_; }

  uint64_t Head() const { return head_; }

  Status Finish();

  size_t CurrentSize() const;

  Status StartGC();

  std::string Name() const;

  Status ParseValidInterval();

  Status PersistenceInterval();

  ~VLog();

 private:
  void IncreaseOffset(const std::string& value);

  Status ParseKeyAndValue(uint64_t* offset, const Slice& from,
                          std::string* key);

  Status ReInsertInVLog(const Slice& key, const Slice& value);

  std::string EncodeEntry(const Slice& key, const Slice& value);

  Status DecodeEntry(uint64_t offset, std::string* key, std::string* value);

  Status InitAllFile();

  DB* db_;  // search in lsm-tree
  const Options* options_;
  Env* env_;

  std::string name_;

  WritableFile* append_vlog_;
  RandomAccessFile* random_read_vlog_;
  RandomWriteFile* random_write_vlog_;

  WritableFile* persistence_write_;  //延迟打开
  SequentialFile* persistence_read_;

  std::string buffer_;

  size_t offset_;
  uint64_t tail_;
  uint64_t head_;
};
}  // namespace leveldb

#endif