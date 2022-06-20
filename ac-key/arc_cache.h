
#ifndef STORAGE_LEVELDB_AC_KEY_ARC_CACHE_H_
#define STORAGE_LEVELDB_AC_KEY_ARC_CACHE_H_

#include "db/dbformat.h"
#include <memory>

#include "leveldb/cache.h"

#include "util/hash.h"

namespace leveldb {

class ARCCache {
 public:
  ARCCache();
  ~ARCCache();

  void SetCapacity(size_t capacity);

  Cache::Handle* Insert(const Slice& key, uint32_t hash, void* value,
                        size_t charge,
                        void (*deleter)(const Slice& key, void* value),
                        const HandleType& handle_type, double caching_factor);
  void Erase(const Slice& key, uint32_t hash);
  Cache::Handle* Lookup(const Slice& key, uint32_t hash);
  void Release(Cache::Handle* e);

 private:
  void Replace(const Slice& key, uint32_t hash, void* value, size_t charge,
               void (*deleter)(const Slice& key, void* value),
               const HandleType& handle_type, double caching_factor);
  std::unique_ptr<LRUCache> t1_lru_;
  std::unique_ptr<LRUCache> t2_lru_;
  std::unique_ptr<LRUCache> t1_ghost_;
  std::unique_ptr<LRUCache> t2_ghost_;
  size_t capacity_;  // t1+t2大小
  int p_;            // t1目标大小 动态变化
};

class ShardedARCCache : public Cache {
 private:
  ARCCache shard_[kNumShards];
  port::Mutex id_mutex_;
  uint64_t last_id_;

  static inline uint32_t HashSlice(const Slice& s) {
    return Hash(s.data(), s.size(), 0);
  }

  static uint32_t Shard(uint32_t hash) { return hash >> (32 - kNumShardBits); }

 public:
  explicit ShardedARCCache(size_t capacity) : last_id_(0) {
    const size_t per_shard = (capacity + (kNumShards - 1)) / kNumShards;
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].SetCapacity(per_shard);
    }
  }
  ~ShardedARCCache() override {}
  Handle* Insert(const Slice& key, void* value, size_t charge,
                 void (*deleter)(const Slice& key, void* value),
                 const HandleType& handle_type,
                 double caching_factor) override {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Insert(key, hash, value, charge, deleter,
                                      handle_type, caching_factor);
  }
  Handle* Lookup(const Slice& key) override {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Lookup(key, hash);
  }
  void Release(Handle* handle) override {
    LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
    shard_[Shard(h->hash)].Release(handle);
  }
  void Erase(const Slice& key) override {
    const uint32_t hash = HashSlice(key);
    shard_[Shard(hash)].Erase(key, hash);
  }
  void* Value(Handle* handle) override {
    return reinterpret_cast<LRUHandle*>(handle)->value;
  }
  uint64_t NewId() override {
    MutexLock l(&id_mutex_);
    return ++(last_id_);
  }
  void Prune() override {}
  size_t TotalCharge() const override {
    size_t total = 0;
    return total;
  }
  bool Contains(const Slice& key) override { return true; }
};

Cache* NewARCCache(size_t capacity);

}  // namespace leveldb

#endif
