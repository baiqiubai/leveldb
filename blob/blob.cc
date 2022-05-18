
#include "blob/blob.h"

#include "leveldb/cache.h"
#include "leveldb/env.h"

#include "table/format.h"
#include "util/coding.h"

namespace leveldb {

struct Blob::Rep {
  Options options_;
  RandomAccessFile* file_;
  uint64_t cache_id_;
  std::string* data_;
  BlockHandle data_handle_;
};

Blob::~Blob() { delete rep_; }

static void DeleteValue(const Slice& k, void* v) {
  std::string* value = reinterpret_cast<std::string*>(v);
  delete value;
}

Status Blob::Open(const Options& options, RandomAccessFile* file,
                  uint64_t file_size, Blob** blobptr) {
  *blobptr = nullptr;
  if (file_size < BlobFooter::kEncodedLength) {
    return Status::Corruption("file is too short to be an blobfile");
  }

  char footer_space[BlobFooter::kEncodedLength];
  Slice footer_input;
  Status s =
      file->Read(file_size - BlobFooter::kEncodedLength,
                 BlobFooter::kEncodedLength, &footer_input, footer_space);
  if (!s.ok()) return s;

  BlobFooter footer;
  s = footer.DecodeFrom(&footer_input);
  if (!s.ok()) return s;

  if (s.ok()) {
    Rep* rep = new Blob::Rep;
    rep->file_ = file;
    rep->options_ = options;
    rep->cache_id_ =
        rep->options_.block_cache ? rep->options_.block_cache->NewId() : 0;
    rep->data_handle_ = footer.data_handle();
    *blobptr = new Blob(rep);
  }
  return s;
}

Slice Blob::DecodeEntry(uint64_t offset) {
  const char* decode_start = rep_->data_->data() + offset;
  uint32_t entry_size = DecodeFixed32(decode_start);
  uint32_t key_size = DecodeFixed32(decode_start + sizeof(uint32_t));
  uint32_t value_size =
      DecodeFixed32(decode_start + sizeof(uint32_t) + key_size);
  return Slice(decode_start + offset + 2 * sizeof(uint32_t), value_size);
}

Status Blob::InternalGet(const ReadOptions& options, const Slice& k, void* arg,
                         void (*handle_result)(void* arg, const Slice& k,
                                               const Slice&)) {
  Status s = BlockReader(this, options, rep_->data_handle_.offset());
  if (s.ok()) {
    uint64_t offset = DecodeFixed64(k.data());
    Slice value = DecodeEntry(offset);
    (*handle_result)(arg, k, value);
  }
}

Status Blob::BlockReader(void* arg, const ReadOptions& options,
                         uint64_t handle_offset) {
  Blob* blob = reinterpret_cast<Blob*>(arg);
  Cache* block_cache = blob->rep_->options_.block_cache;
  Block* block = nullptr;
  Cache::Handle* cache_handle = nullptr;

  Status s;
  BlockContents contents;

  if (block_cache != nullptr) {
    char cache_key_buffer[16];
    EncodeFixed64(cache_key_buffer, blob->rep_->cache_id_);
    EncodeFixed64(cache_key_buffer + 8, handle_offset);
    Slice key(cache_key_buffer, sizeof(cache_key_buffer));
    cache_handle = block_cache->Lookup(key);
    if (cache_handle != nullptr) {
      blob->rep_->data_ =
          reinterpret_cast<std::string*>(block_cache->Value(cache_handle));
    } else {
      s = ReadBlock(blob->rep_->file_, options, rep_->data_handle_, &contents);
      if (s.ok()) {
        blob->rep_->data_ = new std::string(contents.data.ToString());
        if (contents.cachable && options.fill_cache) {
          cache_handle = block_cache->Insert(
              key, rep_->data_, blob->rep_->data_->size(), &DeleteValue);
        }
      }
    }
  }
  return s;
}

}  // namespace leveldb