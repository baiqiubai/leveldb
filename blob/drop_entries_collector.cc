
#include "blob/drop_entries_collector.h"

namespace leveldb {

DropEntriesCollector::DropEntriesCollector(const Options& options)
    : options_(options) {}

void DropEntriesCollector::AddDiscardSize(uint64_t blob_number, uint64_t size) {
  compact_discard_size_[blob_number] += size;
}

void DropEntriesCollector::SetBlobFileSize(uint64_t blob_number,
                                           uint64_t size) {
  blob_file_size_[blob_number] = size;
}

uint64_t DropEntriesCollector::GetBlobSize(uint64_t number) {
  return blob_file_size_[number];
}

void DropEntriesCollector::Clear() {
  blob_file_size_.clear();
  compact_discard_size_.clear();
}

std::unordered_map<uint64_t, uint64_t>
DropEntriesCollector::GetDeleteBlobFileList() {
  std::unordered_map<uint64_t, uint64_t> discard_blob_file;

  if (options_.gc_policy == GCPolicy::kUseDiscardSize) {
    for (auto& [blob_number, discard_size] : compact_discard_size_) {
      double temp_discard_ratio =
          static_cast<double>(discard_size) / blob_file_size_[blob_number];
      if (std::abs(temp_discard_ratio - options_.gc_threshold_ratio) >= kEPS) {
        discard_blob_file.insert({blob_number, blob_file_size_[blob_number]});
      }
    }
  }
  return discard_blob_file;
}

}  // namespace leveldb