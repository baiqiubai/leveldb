
#ifndef STORAGE_LEVELDB_AC_KEY_CACHE_FORMAT_H_
#define STORAGE_LEVELDB_AC_KEY_CACHE_FORMAT_H_

#include "db/dbformat.h"
#include <string>

#include "util/coding.h"

namespace leveldb {

static const int kNumShardBits = 4;
static const int kNumShards = 1 << kNumShardBits;

enum HandleType { kKVHandle = 0x0, kKPHandle = 0x1, kBlockHandle = 0x2 };

enum HitCacheType {
  kKVRealCache = 0x0,
  kKVGhostCache = 0x1,
  kKPRealCache = 0x2,
  kKPGhostCache = 0x3,
  kNoHitCache = 0x4
};

void DecodeKPValue(std::string* src, uint64_t* blob_number,
                   uint64_t* blob_file_size, Slice* result);

void EncodeKPValue(std::string* dst, uint64_t blob_number,
                   uint64_t blob_file_size, uint64_t blob_offset);

void DeleteHandle(const Slice& key, void* v);

double CalculateCachingFactor(int level, const HandleType& type,
                              size_t handle_size);
}  // namespace leveldb

#endif