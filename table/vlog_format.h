

#ifndef STORAGE_LEVELDB_TABLE_VLOG_FORMAT_H_
#define STORAGE_LEVELDB_TABLE_VLOG_FORMAT_H_

#include <future>
#include <memory>
#include <vector>

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

  Status GetUsePromise(uint64_t offset, std::promise<std::string>* value);

  uint64_t ValidTail() const { return tail_; }

  uint64_t ValidHead() const { return head_; }

  Status Finish();

  uint64_t CurrentSize() const;

  Status StartGC();

  std::string Name() const;

  Status ParseValidInterval();

  Status PersistenceInterval();

  ~VLog();

 private:
  Status ParseEntrySize(uint64_t offset, uint32_t* result);

  void ParseValueOrKey(std::string* result, const Slice& key, uint32_t* index);

  Status ReInsertInVLog(const Slice& key, const Slice& value,
                        uint64_t* new_head);

  std::string EncodeEntry(const Slice& key, const Slice& value);

  Status DecodeEntry(uint64_t offset, std::string* key, std::string* value);

  Status GetEntry(uint64_t offset, char* scratch, uint32_t entry_size,
                  Slice* result);

  Status InitAllFile();

  void Flush(std::vector<std::pair<std::string, std::string>>* lists);

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

  uint64_t offset_;
  uint64_t tail_;
  uint64_t head_;

  char entry_buffer_[4096];  //优化
};
}  // namespace leveldb

#endif