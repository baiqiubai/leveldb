#include "table/vlog_format.h"

#include <assert.h>
#include <unistd.h>

#include "include/leveldb/db.h"
namespace leveldb {

std::string makeString(size_t size, bool flag) {
  std::string result;
  char c = 'a';
  if (!flag) c = 'b';
  for (int32_t i = 0; i < size; ++i) {
    result.push_back(c);
  }
  return result;
}

void OpenDB() {
  Options options;
  options.create_if_missing = true;
  DB* db = nullptr;
  Status s = DB::Open(options, "/home/db", &db);
  assert(s.ok());

  std::string key("key");
  /* for (int32_t i = 0; i < 1000; ++i) {
     key += std::to_string(i);
     s = db->Put(WriteOptions(), key, makeString(4500000, true));
     key.pop_back();
     assert(s.ok());
   }*/

  for (int32_t i = 0; i < 10; ++i) {
    s = db->Put(WriteOptions(), makeString(4500000, true), "value");
    assert(s.ok());
  }
  delete db;
  // DestroyDB("/home/db", options);
}
}  // namespace leveldb

int main(int argc, char** argv) {
  leveldb::OpenDB();

  return 0;
}