#include "table/vlog_format.h"

#include "db/filename.h"
#include <iostream>

#include "include/leveldb/options.h"

namespace leveldb {

vLog::vLog(DB* db, const Options* options, Env* env,
           const std::string& vlog_name)
    : db_(db),
      options_(options),
      env_(env),
      name_(vlog_name),
      write_vlog_(nullptr),
      read_vlog_(nullptr),
      persistence_write_(nullptr),
      persistence_read_(nullptr),
      offset_(0),
      tail_(0),
      head_(0) {}

vLog::~vLog() {
  PersistenceInterval();
  delete write_vlog_;
  delete read_vlog_;
  delete persistence_write_;
  delete persistence_read_;
}

void vLog::IncreaseOffset(const std::string& value) { offset_ += value.size(); }

// total_size->value_size->value->key_size->key
// fix32->fix32->char[]->fix32->char[]
std::string vLog::EncodeEntry(const Slice& key, const Slice& value) {
  std::string result;

  std::string key_size, value_size;
  PutFixed32(&key_size, static_cast<uint32_t>(key.size()));
  PutFixed32(&value_size, static_cast<uint32_t>(value.size()));
  uint32_t size =
      key_size.size() + key.size() + value_size.size() + value.size();
  std::string total_size;
  PutFixed32(&total_size, size);

  result += total_size;
  result += value_size;
  result += value.ToString();
  result += key_size;
  result += key.ToString();
  return result;
}

Status vLog::DecodeEntry(uint64_t offset, std::string* key,
                         std::string* value) {
  Slice total;
  char buffer[sizeof(uint32_t)];
  Status s = read_vlog_->Read(offset, sizeof(uint32_t), &total, buffer);
  if (!s.ok()) return s;

  uint32_t index = sizeof(uint32_t);

  uint32_t total_size = DecodeFixed32(total.data());
  Slice entry;
  char entry_buffer[total_size];

  s = read_vlog_->Read(offset + index, total_size, &entry, entry_buffer);
  if (!s.ok()) return s;

  index = 0;
  uint32_t value_size = DecodeFixed32(entry.data() + index);
  index += sizeof(uint32_t);
  *value = std::string(entry.data() + index);
  index += value_size;
  uint32_t key_size = DecodeFixed32(entry.data() + index);
  index += sizeof(uint32_t);
  *key = std::string(entry.data() + index);

  return s;
}

void vLog::Add(const Slice& key, const Slice& value) {
  buffer_ = EncodeEntry(key, value);
  offset_ += buffer_.size();
}

std::string vLog::Name() const { return name_; }

void vLog::Finish() {
  if (write_vlog_ == nullptr) {
    Status s = env_->NewWritableFile(name_, &write_vlog_);
    if (!s.ok()) return;
  }

  Status s = write_vlog_->Append(buffer_.data());
  if (!s.ok()) {
    return;
  }
  write_vlog_->Sync();
  head_ += buffer_.size();
  buffer_.clear();
}

size_t vLog::CurrentSize() const { return offset_; }

Status vLog::Get(uint64_t offset, std::string* value) {
  if (write_vlog_ == nullptr) return Status::Corruption("No data");
  std::string key;
  return DecodeEntry(offset, &key, value);
}

Status vLog::ParseKeyAndValue(uint64_t offset, const Slice& from,
                              std::string* key) {
  uint32_t total_size = DecodeFixed32(from.data());
  char buffer[total_size];
  Slice entry;
  Status s =
      read_vlog_->Read(offset + sizeof(uint32_t), total_size, &entry, buffer);

  if (!s.ok()) return s;

  uint32_t value_size = DecodeFixed32(entry.data());
  uint32_t key_size =
      DecodeFixed32(entry.data() + sizeof(uint32_t) + value_size);

  *key =
      std::string(entry.data() + 2 * sizeof(uint32_t) + value_size, key_size);
  return s;
}

Status vLog::ReInsertInVLog(const Slice& key, const Slice& value) {
  if (random_write_vlog_ == nullptr) {
    Status s = env_->NewRandomWriteFile(name_, &random_write_vlog_);
    if (!s.ok()) return s;
  }
  std::string entry = EncodeEntry(key, value);
  Status s = random_write_vlog_->Write(entry, entry.size(), head_);
  if (!s.ok()) return s;

  random_write_vlog_->Sync();
  head_ += entry.size();
  return s;
}

Status vLog::StartGC() {
  if (read_vlog_ == nullptr) {
    Status s = env_->NewRandomAccessFile(name_, &read_vlog_);
    if (!s.ok()) return s;
  }

  uint64_t parse_offset = tail_;
  uint64_t last_head = head_;
  Status s;
  constexpr int size = sizeof(uint64_t);
  char buffer[size] = {'\0'};

  while (parse_offset <= last_head) {
    Slice result;
    s = read_vlog_->Read(parse_offset, sizeof(uint64_t), &result, buffer);
    if (!s.ok()) return s;
    std::string key;
    s = ParseKeyAndValue(parse_offset, result, &key);
    if (!s.ok()) return s;

    std::string value;

    // find in lsm-tree
    if (db_->Get(ReadOptions(), key, &value).ok()) {
      uint32_t offset = DecodeFixed32(value.data());
      s = ReInsertInVLog(key, value);
      if (!s.ok()) return s;
    }
  }
  s = random_write_vlog_->Fallocate(tail_, last_head - tail_);
  if (!s.ok()) return s;
  tail_ = last_head;
  return s;
}

Status vLog::ParseValidInterval() {
  std::string interval_file_name = vLogValidIntervalFileName(db_->GetName());

  if (persistence_read_ == nullptr) {
    Status s = env_->NewSequentialFile(interval_file_name, &persistence_read_);
    if (!s.ok()) return s;
  }
  Slice result;
  char buffer[2 * sizeof(uint64_t)];
  Status s = persistence_read_->Read(2 * sizeof(uint64_t), &result, buffer);
  if (!s.ok()) return s;

  tail_ = DecodeFixed64(result.data());
  head_ = DecodeFixed64(result.data() + sizeof(uint64_t));

  Log(options_->info_log, "Valid Interval [%ld->%ld]", tail_, head_);

  return s;
}

Status vLog::PersistenceInterval() {
  std::string interval_file_name = vLogValidIntervalFileName(db_->GetName());

  bool first_persistence = (persistence_write_ == nullptr) ? true : false;

  std::string file_name = interval_file_name;

  if (!first_persistence) {
    file_name += ".new";
  }

  if (!first_persistence) delete persistence_write_;

  Status s = env_->NewWritableFile(file_name, &persistence_write_);
  if (!s.ok()) return s;

  std::string head_str, tail_str;
  PutFixed64(&head_str, static_cast<uint64_t>(head_));
  PutFixed64(&tail_str, static_cast<uint64_t>(tail_));

  std::string buffer = tail_str;
  buffer += head_str;
  s = persistence_write_->Append(buffer);
  if (s.ok()) {
    persistence_write_->Sync();
  }

  if (!first_persistence) {
    Status s = env_->RenameFile(file_name, interval_file_name);
    if (!s.ok()) return s;
  }

  fprintf(stderr, "Persistence Valid Interval [%ld->%ld]", tail_, head_);

  return s;
}
}  // namespace leveldb