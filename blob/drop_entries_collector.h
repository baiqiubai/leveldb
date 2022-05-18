

#ifndef STORAGE_LEVELDB_BLOB_DROP_ENTRIES_COLLECTOR_H_
#define STORAGE_LEVELDB_BLOB_DROP_ENTRIES_COLLECTOR_H_

#include <cstdint>
#include <unordered_map>
#include <unordered_set>

#include "include/leveldb/options.h"

namespace leveldb {

class DropEntriesCollector {
 public:
  DropEntriesCollector(const Options& options);
  ~DropEntriesCollector() = default;

  DropEntriesCollector(const DropEntriesCollector&) = delete;
  DropEntriesCollector& operator=(const DropEntriesCollector&) = delete;

  std::unordered_set<uint64_t> GetDeleteBlobFileList();

  void AddDiscardSize(uint64_t blob_number, uint64_t size);

 private:
  static constexpr double kEPS = 1e-7;
  const Options options_;
  std::unordered_map<uint64_t, uint64_t> compact_discard_size_;
  std::unordered_map<uint64_t, uint64_t> blob_file_size_;
};
}  // namespace leveldb

#endif
