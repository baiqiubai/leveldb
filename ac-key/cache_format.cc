
#include "ac-key/cache_format.h"

namespace leveldb {
// KPCache Value format blob_number->blob_file_size->blob_offset
void DecodeKPValue(std::string* src, uint64_t* blob_number,
                   uint64_t* blob_file_size, Slice* result) {
  *blob_number = DecodeFixed64(src->data());
  *blob_file_size = DecodeFixed64(src->data() + sizeof(uint64_t));
  *result = Slice(src->data() + 2 * sizeof(uint64_t), sizeof(uint64_t));
}

void EncodeKPValue(std::string* dst, uint64_t blob_number,
                   uint64_t blob_file_size, uint64_t blob_offset) {
  PutFixed64(dst, blob_number);
  PutFixed64(dst, blob_file_size);
  PutFixed64(dst, blob_offset);
}

void DeleteHandle(const Slice& key, void* v) {
  delete reinterpret_cast<std::string*>(v);
}

double CalculateCachingFactor(int level, const HandleType& type,
                              size_t handle_size) {
  int num_of_sst_to_read = 0;
  int num_of_blob_to_read = 1;
  int num_of_save_io = 0;

  //假设目标key在level层
  if (level == 0) {
    num_of_sst_to_read = config::kL0_StopWritesTrigger / 2;
  } else {
    num_of_sst_to_read = config::kL0_StopWritesTrigger + level;
  }

  switch (type) {
    case HandleType::kBlockHandle: {
      num_of_save_io = 1;
      break;
    }
    case HandleType::kKPHandle: {
      num_of_save_io = num_of_sst_to_read + 2;  //相比较kv需要多一次io
      break;
    }
    case HandleType::kKVHandle: {
      num_of_save_io = num_of_sst_to_read + 2 +
                       1;  //节约的io 读取m个sst中布隆过滤器
                           //一个index_block一个data_block以及一个blob_file
      break;
    }
    default:
      break;
  }
  return num_of_save_io / handle_size;
}
}  // namespace leveldb
