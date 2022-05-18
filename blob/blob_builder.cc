
#include "blob_builder.h"

#include "leveldb/env.h"

#include "table/format.h"
#include "util/coding.h"

namespace leveldb {

BlobBuilder::BlobBuilder(const Options& options, WritableFile* file)
    : options_(options), file_(file), offset_(0) {}

BlobBuilder::~BlobBuilder() {}

void BlobBuilder::Add(const Slice& key, const Slice& value) {
  std::string entry = DecodeEntry(key, value);
  buffer_ += entry;
  offset_ += static_cast<uint64_t>(entry.size());
}

uint64_t BlobBuilder::CurrentSizeEstimate() const { return offset_; }

uint64_t BlobBuilder::FileSize() const { return offset_; }

std::string BlobBuilder::DecodeEntry(const Slice& key, const Slice& value) {
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
  result += key_size;
  result += key.ToString();
  result += value_size;
  result += value.ToString();

  return result;
}

Status BlobBuilder::WriteFooter() {
  BlobFooter footer;
  std::string decode_footer;
  footer.set_data_handle(pending_handle_);
  footer.EncodeTo(&decode_footer);

  Status s = file_->Append(Slice(decode_footer));
  if (s.ok()) {
    offset_ += static_cast<uint64_t>(decode_footer.size());
  }
  return s;
}

Status BlobBuilder::Finish() {
  Slice raw_contents(buffer_);
  CompressionType type = options_.compression;
  std::string compressed;

  // Slice block_contents = GetCompressBlock(raw_contents, &type, &compressed);
  // do not compress
  WriteRawBlock(raw_contents, file_, type, &pending_handle_, &offset_);
  offset_ -= static_cast<uint64_t>(raw_contents.size());
  assert(offset_ == raw_contents.size() + kBlockTrailerSize);
  Status s = WriteFooter();

  if (!s.ok()) {
    return s;
  }
  return file_->Flush();
}

}  // namespace leveldb