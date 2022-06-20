
#include "ac-key/arc_cache.h"

namespace leveldb {

Cache* NewARCCache(size_t capacity) { return new ShardedARCCache(capacity); }

ARCCache::ARCCache() : capacity_(0), p_(0) {
  t1_lru_.reset(new LRUCache());
  t2_lru_.reset(new LRUCache());
  t1_ghost_.reset(new LRUCache());
  t2_ghost_.reset(new LRUCache());
  t1_lru_->UseCachingFactor();
  t2_lru_->UseCachingFactor();
  t1_ghost_->UseCachingFactor();
  t2_ghost_->UseCachingFactor();
}

ARCCache::~ARCCache() {
  p_ = 0;
  capacity_ = 0;
}

void ARCCache::SetCapacity(size_t capacity) {
  capacity_ = capacity;
  t1_lru_->SetCapacity(capacity_);
  t2_lru_->SetCapacity(capacity_);
  t1_ghost_->SetCapacity(capacity_);
  t2_ghost_->SetCapacity(capacity_);
}

Cache::Handle* ARCCache::Insert(const Slice& key, uint32_t hash, void* value,
                                size_t charge,
                                void (*deleter)(const Slice& key, void* value),
                                const HandleType& handle_type,
                                double caching_factor) {
  // case I
  if (t1_lru_->Contains(key, hash)) {
    t1_lru_->Erase(key, hash);
    return t2_lru_->Insert(key, hash, value, charge, deleter, handle_type,
                           caching_factor);
  }
  if (t2_lru_->Contains(key, hash)) {
    t2_lru_->Erase(key, hash);
    return t2_lru_->Insert(key, hash, value, charge, deleter, handle_type,
                           caching_factor);
  }
  // case II
  if (t1_ghost_->Contains(key, hash)) {
    size_t ghost_t1_size = t1_ghost_->TotalCharge();
    size_t ghost_t2_size = t2_ghost_->TotalCharge();
    size_t diff = 0;

    if (ghost_t1_size > ghost_t2_size) {
      diff = 1;
    } else {
      diff = ghost_t2_size / ghost_t1_size;
    }
    p_ = std::min(capacity_, p_ + diff);
    Replace(key, hash, value, charge, deleter, handle_type, caching_factor);

    t1_ghost_->Erase(key, hash);
    return t2_lru_->Insert(key, hash, value, charge, deleter, handle_type,
                           caching_factor);
  }
  // case III
  if (t2_ghost_->Contains(key, hash)) {
    size_t ghost_t1_size = t1_ghost_->TotalCharge();
    size_t ghost_t2_size = t2_ghost_->TotalCharge();
    size_t diff = 0;

    if (ghost_t1_size > ghost_t2_size) {
      diff = 1;
    } else {
      diff = ghost_t2_size / ghost_t1_size;
    }
    p_ = std::min(static_cast<size_t>(0), p_ - diff);
    Replace(key, hash, value, charge, deleter, handle_type, caching_factor);

    t2_ghost_->Erase(key, hash);
    return t2_lru_->Insert(key, hash, value, charge, deleter, handle_type,
                           caching_factor);
  }
  // Case IV
  size_t l1_ghost_size = t1_ghost_->TotalCharge();
  size_t l1_lru_size = t1_lru_->TotalCharge();
  size_t l1_size = l1_ghost_size + l1_lru_size;

  if (l1_size == capacity_) {
    if (l1_lru_size < capacity_) {
      t1_ghost_->Erase(key, hash);
      Replace(key, hash, value, charge, deleter, handle_type, caching_factor);
    } else {  //需要从t1剔除 否则会导致arc过大
      t1_lru_->Erase(key, hash);
    }
  } else {
    size_t l2_ghost_size = t2_ghost_->TotalCharge();
    size_t l2_lru_size = t2_lru_->TotalCharge();
    size_t l2_size = l2_ghost_size + l2_lru_size;

    if (l1_size + l2_size >= capacity_) {
      t2_ghost_->Erase(key, hash);
      if (l1_size + l2_lru_size == 2 * capacity_) {
        Replace(key, hash, value, charge, deleter, handle_type, caching_factor);
      }
    }
  }
  return t1_lru_->Insert(key, hash, value, charge, deleter, handle_type,
                         caching_factor);
}

void ARCCache::Erase(const Slice& key, uint32_t hash) {
  if (t1_lru_->Contains(key, hash)) {
    t1_lru_->Erase(key, hash);
  }

  if (t2_lru_->Contains(key, hash)) {
    t2_lru_->Erase(key, hash);
  }

  if (t1_ghost_->Contains(key, hash)) {
    t1_ghost_->Erase(key, hash);
  }

  if (t2_ghost_->Contains(key, hash)) {
    t2_ghost_->Erase(key, hash);
  }
}

void ARCCache::Replace(const Slice& key, uint32_t hash, void* value,
                       size_t charge,
                       void (*deleter)(const Slice& key, void* value),
                       const HandleType& handle_type, double caching_factor) {
  size_t t1_size = t1_lru_->TotalCharge();

  if (t1_size > 0 && (t1_size > p_) ||
      (t2_ghost_->Contains(key, hash) && t1_size == p_)) {
    t1_lru_->Erase(key, hash);
    t1_ghost_->Insert(key, hash, nullptr, charge, deleter, handle_type,
                      caching_factor);
  } else {
    t2_lru_->Erase(key, hash);
    t2_ghost_->Insert(key, hash, nullptr, charge, deleter, handle_type,
                      caching_factor);
  }
}

Cache::Handle* ARCCache::Lookup(const Slice& key, uint32_t hash) {
  LRUHandle* handle = nullptr;

  if ((handle = reinterpret_cast<LRUHandle*>(t1_lru_->Lookup(key, hash))) !=
      nullptr) {
    handle->hit_cache_type = HitCacheType::kKPRealCache;
    return reinterpret_cast<Cache::Handle*>(handle);
  }

  if ((handle = reinterpret_cast<LRUHandle*>(t2_lru_->Lookup(key, hash))) !=
      nullptr) {
    handle->hit_cache_type = HitCacheType::kKVRealCache;
    return reinterpret_cast<Cache::Handle*>(handle);
  }

  if ((handle = reinterpret_cast<LRUHandle*>(t1_ghost_->Lookup(key, hash))) !=
      nullptr) {
    handle->hit_cache_type = HitCacheType::kKPGhostCache;
    return reinterpret_cast<Cache::Handle*>(handle);
  }

  if ((handle = reinterpret_cast<LRUHandle*>(t2_ghost_->Lookup(key, hash))) !=
      nullptr) {
    handle->hit_cache_type = HitCacheType::kKVGhostCache;
    return reinterpret_cast<Cache::Handle*>(handle);
  }
  return nullptr;
}

void ARCCache::Release(Cache::Handle* e) {
  LRUHandle* handle = reinterpret_cast<LRUHandle*>(e);
  Slice key = handle->key();
  uint32_t hash = handle->hash;

  if (t1_lru_->Contains(key, hash)) {
    t1_lru_->Release(e);
  }

  if (t2_lru_->Contains(key, hash)) {
    t2_lru_->Release(e);
  }

  if (t1_ghost_->Contains(key, hash)) {
    t1_ghost_->Release(e);
  }

  if (t2_ghost_->Contains(key, hash)) {
    t1_ghost_->Release(e);
  }
}
}  // namespace leveldb
