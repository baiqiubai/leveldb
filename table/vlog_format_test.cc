#include "table/vlog_format.h"

#include "db/db_impl.h"
#include <assert.h>
#include <iostream>
#include <map>
#include <unistd.h>
#include <vector>

#include "util/prefetcher.h"

#include "gtest/gtest.h"

namespace leveldb {

static const std::string str = "sdjkdfnkldsjlfkjkwkero";

std::string makeString(size_t size) {
  std::string result;
  srand(clock());
  for (int32_t i = 0; i < size; ++i) {
    int index = rand() % str.size();
    result.push_back(str[index]);
  }
  return result;
}

void Validation(const std::vector<std::string>& from,
                const std::vector<std::string>& result, int n) {
  for (int i = 0; i < n; ++i) {
    fprintf(stderr, "from->%s,result->%s\n", from[i].data(), result[i].data());
    // ASSERT_EQ(from[i], result[i]);
  }
}

void MakeRandomKV(std::set<std::string>* keys, std::vector<std::string>* values,
                  int n) {
  for (int i = 0; i < n; ++i) {
    std::string key = makeString(n);
    if (!keys->count(key)) {
      std::string value = makeString(n);
      keys->insert(key);
      values->emplace_back(value);
    }
  }
}

namespace {
std::set<std::string> keys;
std::vector<std::string> values;
std::vector<std::string> result;
Options options;
int num = 10;
}  // namespace

DB* ConfigDB() {
  DB* db = nullptr;
  result.clear();
  options.create_if_missing = true;
  Status s = DB::Open(options, "/home/db", &db);
  assert(s.ok());
  MakeRandomKV(&keys, &values, num);
  return db;
}

/*
TEST(TestKVSeparation, ScanAllMemTable) {
  DB* db = ConfigDB();

  Status s;
  auto iter = keys.begin();
  num = keys.size();  //重新调整正确size

  for (int32_t i = 0; i < num; ++i) {
    s = db->Put(WriteOptions(), *iter, values[i]);
    ASSERT_TRUE(s.ok());
    ++iter;
  }

  s = reinterpret_cast<DBImpl*>(db)->ScanCountOfValue(
      ReadOptions(), *keys.begin(), num, &result);
  ASSERT_TRUE(s.ok());

  Validation(values, result, num);

  s = DestroyDB("/home/db", options);
}
*/
TEST(TestKVSepration, ScanAllSST) {
  DB* db = ConfigDB();

  Status s;
  auto iter = keys.begin();
  num = keys.size();  //重新调整正确size

  for (int32_t i = 0; i < num; ++i) {
    s = db->Put(WriteOptions(), *iter, values[i]);
    ASSERT_TRUE(s.ok());
    ++iter;
  }

  s = reinterpret_cast<DBImpl*>(db)->TEST_CompactMemTable();
  ASSERT_TRUE(s.ok());

  s = reinterpret_cast<DBImpl*>(db)->ScanCountOfValue(
      ReadOptions(), *keys.begin(), num, &result);
  ASSERT_TRUE(s.ok());

  Validation(values, result, num);

  delete db;
}

}  // namespace leveldb

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}