
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
  BlobBlock* blob_block_;
  BlockHandle data_handle_;
};

Blob::~Blob() { delete rep_; }

class BlobBlock::Iter : public Iterator {
 public:
  Iter(const char* data, uint64_t end)
      : data_(data), end_(end), current_(0), entry_size_(0) {}

  Slice value() const override {
    assert(Valid());
    return value_;
  }

  Slice key() const override {
    assert(Valid());
    return key_;
  }

  bool Valid() const override { return current_ < end_; }
  void SeekToFirst() override;
  void SeekToLast() override;
  void Seek(const Slice& target) override;
  void Next() override;
  void Prev() override;

  Status status() const override { return status_; }

 private:
  const char* data_;

  uint64_t end_;
  uint64_t current_;
  uint32_t entry_size_;
  Status status_;
  Slice key_;
  Slice value_;
  void DecodeEntry(uint64_t offset);
  void SeekByOffset(uint64_t offset);
};

void BlobBlock::Iter::DecodeEntry(uint64_t offset) {
  const char* decode_start = data_ + offset;
  uint32_t entry_size_ = DecodeFixed32(decode_start);
  uint32_t key_size = DecodeFixed32(decode_start + sizeof(uint32_t));
  uint32_t value_size =
      DecodeFixed32(decode_start + 2 * sizeof(uint32_t) + key_size);

  key_ = Slice(decode_start + 2 * sizeof(uint32_t), key_size);
  value_ = Slice(decode_start + 3 * sizeof(uint32_t) + key_size, value_size);
}

void BlobBlock::Iter::SeekToFirst() { DecodeEntry(0); }

void BlobBlock::Iter::Next() {
  current_ += entry_size_;
  DecodeEntry(current_);
}

void BlobBlock::Iter::SeekToLast() {}

void BlobBlock::Iter::Prev() {}

void BlobBlock::Iter::Seek(const Slice& data) {
  uint64_t offset = DecodeFixed64(data.data());
  SeekByOffset(offset);
}

void BlobBlock::Iter::SeekByOffset(uint64_t offset) {
  assert(offset < end_);
  DecodeEntry(offset);
  current_ = offset;
}

BlobBlock::BlobBlock(const BlockContents& contents)
    : data_(contents.data.data()),
      size_(contents.data.size()),
      owner_(contents.heap_allocated) {}

BlobBlock::~BlobBlock() {
  if (owner_) {
    delete[] data_;
  }
}

size_t BlobBlock::size() const { return size_; }

Iterator* BlobBlock::NewIterator(const ReadOptions& options) const {
  return new Iter(data_, size_);
}

static void DeleteBlock(void* arg, void* ignored) {
  delete reinterpret_cast<BlobBlock*>(arg);
}

static void DeleteCachedBlock(const Slice& key, void* value) {
  BlobBlock* block = reinterpret_cast<BlobBlock*>(value);
  delete block;
}

static void ReleaseBlock(void* arg, void* h) {
  Cache* cache = reinterpret_cast<Cache*>(arg);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
  cache->Release(handle);
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
  if (!s.ok()) {
    return s;
  }

  BlobFooter footer;
  s = footer.DecodeFrom(&footer_input);
  if (!s.ok()) {
    return s;
  }

  if (s.ok()) {
    Rep* rep = new Blob::Rep;
    rep->file_ = file;
    rep->options_ = options;
    rep->blob_block_ = nullptr;
    rep->cache_id_ =
        rep->options_.block_cache ? rep->options_.block_cache->NewId() : 0;
    rep->data_handle_ = footer.data_handle();
    *blobptr = new Blob(rep);
  }
  return s;
}

Status Blob::InternalGet(const ReadOptions& options, const Slice& k, void* arg,
                         void (*handle_result)(void* arg, const Slice& k,
                                               const Slice&)) {
  Iterator* iter = BlockReader(this, options, rep_->data_handle_.offset());
  iter->Seek(k);
  if (iter->Valid()) {
    (*handle_result)(arg, k, iter->value());
  } else {
    return iter->status();
  }
  delete iter;
  return Status::OK();
}

Iterator* Blob::BlockReader(void* arg, const ReadOptions& options,
                            uint64_t handle_offset) {
  Blob* blob = reinterpret_cast<Blob*>(arg);
  Cache* block_cache = blob->rep_->options_.block_cache;
  BlobBlock* block = nullptr;
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
      block = reinterpret_cast<BlobBlock*>(block_cache->Value(cache_handle));
    } else {
      s = ReadBlock(blob->rep_->file_, options, rep_->data_handle_, &contents);
      if (s.ok()) {
        block = new BlobBlock(contents);
        if (contents.cachable && options.fill_cache) {
          cache_handle =
              block_cache->Insert(key, block, block->size(), &DeleteCachedBlock,
                                  HandleType::kBlockHandle, 0);
        }
      }
    }
  } else {
    s = ReadBlock(blob->rep_->file_, options, rep_->data_handle_, &contents);
    if (s.ok()) {
      block = new BlobBlock(contents);
    }
  }

  Iterator* iter;
  if (block != nullptr) {
    iter = block->NewIterator(options);
    if (cache_handle == nullptr) {
      iter->RegisterCleanup(&DeleteBlock, block, nullptr);
    } else {
      iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
    }
  } else {
    return NewErrorIterator(s);
  }

  return iter;
}

Iterator* Blob::NewIterator(const ReadOptions& options) {
  return new BlobBlock::Iter(
      nullptr, 0);  //这里不需要使用Blob iter仅仅需要析构的时候调用回调函数即可
}

}  // namespace leveldb
