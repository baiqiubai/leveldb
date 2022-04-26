#include <memory>

#include "leveldb/slice.h"

#include "util/coding.h"

#include "include/leveldb/db.h"
#include "include/leveldb/env.h"

namespace leveldb {
class vLog {
 public:
  explicit vLog(DB* db, const Options* options, Env* env,
                const std::string& vlog_name);

  vLog(const vLog&) = delete;
  vLog& operator=(const vLog&) = delete;

  void Add(const Slice& key, const Slice& value);

  Status Get(uint64_t offset, std::string* value);

  uint64_t Tail() const { return tail_; }

  uint64_t Head() const { return head_; }

  void Finish();

  size_t CurrentSize() const;

  Status StartGC();

  std::string Name() const;

  Status ParseValidInterval();

  Status PersistenceInterval();

  ~vLog();

 private:
  void IncreaseOffset(const std::string& value);

  Status ParseKeyAndValue(uint64_t offset, const Slice& from, std::string* key);

  Status ReInsertInVLog(const Slice& key, const Slice& value);

  std::string EncodeEntry(const Slice& key, const Slice& value);

  Status DecodeEntry(uint64_t offset, std::string* key, std::string* value);

  DB* db_;  // search in lsm-tree
  const Options* options_;
  Env* env_;

  std::string name_;

  WritableFile* write_vlog_;
  RandomAccessFile* read_vlog_;
  RandomWriteFile* random_write_vlog_;

  WritableFile* persistence_write_;
  SequentialFile* persistence_read_;

  std::string buffer_;

  size_t offset_;
  uint64_t tail_;
  uint64_t head_;
};
}  // namespace leveldb