#include "table/vlog_format.h"

#include "db/db_impl.h"
#include "db/filename.h"

#include "util/coding.h"

#include "include/leveldb/db.h"
#include "include/leveldb/env.h"
#include "include/leveldb/options.h"
#include "include/leveldb/slice.h"

namespace leveldb {

VLog::VLog(DB* db, const Options* options, Env* env,
           const std::string& vlog_name)
    : db_(db),
      options_(options),
      env_(env),
      name_(vlog_name),
      random_write_vlog_(nullptr),
      random_read_vlog_(nullptr),
      persistence_write_(nullptr),
      persistence_read_(nullptr),
      offset_(0),
      tail_(0),
      head_(0) {
  Status s = InitAllFile();
  if (!s.ok()) {
    Log(options_->info_log, "Init Vlog file error %s", s.ToString().data());
  }
}

VLog::~VLog() {
  PersistenceInterval();
  delete append_vlog_;
  delete random_write_vlog_;
  delete random_read_vlog_;
  delete persistence_write_;
  delete persistence_read_;
}

Status VLog::InitAllFile() {
  Status s;
  s = env_->NewAppendableFile(name_, &append_vlog_);
  if (!s.ok()) {
    return s;
  }

  s = env_->NewRandomWriteFile(name_, &random_write_vlog_);
  if (!s.ok()) {
    return s;
  }

  s = env_->NewRandomAccessFile(name_, &random_read_vlog_);
  return s;
}
// total_size->value_size->value->key_size->key
// fix32->fix32->char[]->fix32->char[]
std::string VLog::EncodeEntry(const Slice& key, const Slice& value) {
  std::string key_size, value_size;

  PutFixed32(&key_size, static_cast<uint32_t>(key.size()));
  PutFixed32(&value_size, static_cast<uint32_t>(value.size()));

  uint32_t size = static_cast<uint32_t>(key_size.size()) +
                  static_cast<uint32_t>(key.size()) +
                  static_cast<uint32_t>(value_size.size()) +
                  static_cast<uint32_t>(value.size());
  std::string total_size;
  PutFixed32(&total_size, size);

  std::string result;
  result.reserve(size + sizeof(uint32_t));

  result += total_size;
  result += value_size;
  result += value.ToString();
  result += key_size;
  result += key.ToString();

  return result;
}

Status VLog::ParseEntrySize(uint64_t offset, uint32_t* result) {
  Slice total;
  char buffer[sizeof(uint32_t)];

  Status s = random_read_vlog_->Read(offset, sizeof(uint32_t), &total, buffer);
  if (!s.ok()) {
    return s;
  }

  *result = DecodeFixed32(total.data());
  return s;
}

void VLog::ParseValueOrKey(std::string* result, const Slice& entry,
                           uint32_t* index) {
  uint32_t result_size = DecodeFixed32(entry.data() + *index);
  *index += sizeof(uint32_t);
  *result = std::string(entry.data() + *index, result_size);
  *index += result_size;
}

Status VLog::DecodeEntry(uint64_t offset, std::string* key,
                         std::string* value) {
  uint32_t entry_size = 0;

  Status s = ParseEntrySize(offset, &entry_size);
  if (!s.ok()) {
    return s;
  }

  Slice entry;
  uint32_t index = sizeof(uint32_t);
  offset += index;

  char* buffer = nullptr;
  bool use_stack = true;

  if (entry_size < 4096) {
    s = GetEntry(offset, entry_buffer_, entry_size, &entry);

  } else {
    char* buffer = new char[entry_size];
    s = GetEntry(offset, buffer, entry_size, &entry);
    use_stack = false;
  }

  if (!s.ok()) {
    goto out;
  }

  index = 0;
  ParseValueOrKey(value, entry, &index);
  ParseValueOrKey(key, entry, &index);

out:
  if (!use_stack) {
    delete buffer;
  }
  return s;
}

//此处scratch必须在作用域外 实际做的操作是result的data指向scratch的起始位置
Status VLog::GetEntry(uint64_t offset, char* scratch, uint32_t entry_size,
                      Slice* result) {
  return random_read_vlog_->Read(offset, entry_size, result, scratch);
}

void VLog::Add(const Slice& key, const Slice& value) {
  std::string entry = EncodeEntry(key, value);
  buffer_ += entry;
  offset_ += static_cast<uint64_t>(entry.size());
}

std::string VLog::Name() const { return name_; }

Status VLog::Finish() {
  Status s = append_vlog_->Append(
      buffer_);  //不能写成buffer.data()
                 //这里由于构造Slice采用strlen判断大小导致后面都截断
  if (!s.ok()) {
    return s;
  }
  s = append_vlog_->Sync();
  if (s.ok()) {
    head_ += static_cast<uint64_t>(buffer_.size());
  }

  Log(options_->info_log, "[tail:%ld->head:%ld]", tail_, head_);

  buffer_.clear();

  return s;
}

uint64_t VLog::CurrentSize() const { return offset_; }

Status VLog::Get(uint64_t offset, std::string* value) {
  std::string key;
  return DecodeEntry(offset, &key, value);
}

Status VLog::GetUsePromise(uint64_t offset, std::promise<std::string>* value) {
  std::string temp_value;
  std::string key;
  Status s = DecodeEntry(offset, &key, &temp_value);
  value->set_value(std::move(temp_value));
  return s;
}

Status VLog::ReInsertInVLog(const Slice& key, const Slice& value,
                            uint64_t* new_head) {
  std::string entry = EncodeEntry(key, value);
  Status s = random_write_vlog_->Write(entry, entry.size(), *new_head);
  if (!s.ok()) {
    return s;
  }

  s = random_write_vlog_->Sync();
  if (s.ok()) {
    *new_head += static_cast<uint64_t>(entry.size());
  }
  return s;
}

Status VLog::StartGC() {
  if (tail_ == head_) return Status::OK();

  uint64_t parse_offset = tail_;
  uint64_t end_offset = head_;
  uint64_t new_offset = head_;
  Status s;
  std::vector<std::pair<std::string, std::string>> lists;
  uint64_t total_size = 0;
  ReadOptions readoptions;

  Log(options_->info_log, "Start GC [tail:%ld->head:%ld]", parse_offset,
      end_offset);

  while (parse_offset < end_offset) {
    std::string key;
    std::string value_offset;

    s = DecodeEntry(parse_offset, &key, &value_offset);
    if (!s.ok()) {
      return s;
    }

    ParsedInternalKey parse_key;
    if (!ParseInternalKey(key, &parse_key)) {
      return Status::Corruption("ParseInternalKey key:%s error", key.data());
    }

    std::string value;
    bool need_reinsert = true;
    std::string no_sequence_key = parse_key.user_key.ToString();
    std::unique_ptr<Snapshot> temp_snapshot(
        new SnapshotImpl(parse_key.sequence));
    readoptions.snapshot = temp_snapshot.get();

    s = db_->Get(readoptions, no_sequence_key, &value);

    if (!s.ok()) {
      need_reinsert = false;
    }

    if (need_reinsert) {
      std::string offset_str;
      uint64_t temp_offset = new_offset;
      PutFixed64(&offset_str, new_offset);
      s = ReInsertInVLog(key, value, &new_offset);
      if (!s.ok()) {
        Log(options_->info_log, "ReInsert K-V in VLOG error:%s",
            s.ToString().c_str());
        return s;
      } else {
        Log(options_->info_log,
            "ReInsert K-V in VLOG success,user_key:%s,sequence %ld",
            no_sequence_key.data(), parse_key.sequence);
      }
      total_size += static_cast<uint64_t>(key.size());
      total_size += static_cast<uint64_t>(value.size());
      lists.push_back({no_sequence_key, offset_str});
      if (total_size > options_->write_buffer_size) {
        Flush(&lists);
        total_size = 0;
        lists.clear();
      }
      head_ = new_offset;
    }
    parse_offset += sizeof(uint32_t) * 3;
    parse_offset += static_cast<uint64_t>(key.size());
    parse_offset += static_cast<uint64_t>(value_offset.size());
  }

  s = random_write_vlog_->Fallocate(tail_, end_offset - tail_);
  if (!s.ok()) {
    return s;
  }

  tail_ = end_offset;

  return s;
}

void VLog::Flush(std::vector<std::pair<std::string, std::string>>* lists) {
  DBImpl* db = reinterpret_cast<DBImpl*>(db_);
  db->AddSequenceNumberInKeys(lists);
  const Options* options = options_;
  std::sort(lists->begin(), lists->end(),
            [&options](const std::pair<std::string, std::string>& p1,
                       const std::pair<std::string, std::string>& p2) {
              return options->comparator->Compare(p1.first, p2.first) < 0;
            });
  db->CompactEntryList(*lists);
}

Status VLog::ParseValidInterval() {
  std::string interval_file_name =
      vLogValidIntervalFileName(reinterpret_cast<DBImpl*>(db_)->GetName());

  if (persistence_read_ == nullptr) {
    Status s = env_->NewSequentialFile(interval_file_name, &persistence_read_);
    if (!s.ok()) {
      return s;
    }
  }

  Slice result;
  char buffer[2 * sizeof(uint64_t)];

  Status s = persistence_read_->Read(2 * sizeof(uint64_t), &result, buffer);
  if (!s.ok()) {
    return s;
  }

  tail_ = DecodeFixed64(result.data());
  head_ = DecodeFixed64(result.data() + sizeof(uint64_t));
  offset_ = head_;

  Log(options_->info_log, "Valid Interval [%ld->%ld]", tail_, head_);

  return s;
}

Status VLog::PersistenceInterval() {
  std::string interval_file_name =
      vLogValidIntervalFileName(reinterpret_cast<DBImpl*>(db_)->GetName());

  bool first_persistence = (persistence_write_ == nullptr) ? true : false;

  std::string file_name = interval_file_name;

  if (!first_persistence) {
    file_name += ".new";
  }

  if (!first_persistence) {
    delete persistence_write_;
  }

  Status s = env_->NewWritableFile(file_name, &persistence_write_);
  if (!s.ok()) {
    return s;
  }

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
    if (!s.ok()) {
      return s;
    }
  }

  return s;
}
}  // namespace leveldb