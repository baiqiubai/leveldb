// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "db/builder.h"

#include "db/dbformat.h"
#include "db/filename.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include <iostream>

#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"

#include "table/vlog_format.h"

namespace leveldb {

static Slice GetLengthPrefixedSlice(const char* data) {
  uint32_t len;
  const char* p = data;
  p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted
  return Slice(p, len);
}

Status BuildTable(
    const std::string& dbname, Env* env, const Options& options,
    TableCache* table_cache, Iterator* iter, FileMetaData* meta, VLog* vlog,
    const std::vector<std::pair<std::string, std::string>>& lists) {
  Status s;
  meta->file_size = 0;

  bool compact_memtable = (iter != nullptr) ? true : false;

  if (compact_memtable) {
    iter->SeekToFirst();
  }

  std::string fname = TableFileName(dbname, meta->number);

  if ((compact_memtable && iter->Valid()) || !compact_memtable) {
    WritableFile* file;
    s = env->NewWritableFile(fname, &file);
    if (!s.ok()) {
      return s;
    }

    TableBuilder* builder = new TableBuilder(options, file);
    if (compact_memtable) {
      meta->smallest.DecodeFrom(iter->key());
    } else {
      meta->smallest.DecodeFrom(
          GetLengthPrefixedSlice(lists.begin()->first.data()));
    }

    Slice key;
    if (compact_memtable) {
      for (; iter->Valid(); iter->Next()) {
        key = iter->key();
        std::string offset;
        {
          ParsedInternalKey parsed_key;
          if (ParseInternalKey(key, &parsed_key)) {
            Log(options.info_log, "Flush user_key %s,sequence %ld",
                parsed_key.user_key.data(), parsed_key.sequence);
          }
        }
        PutFixed64(&offset, vlog->CurrentSize());
        builder->Add(key, offset);  // value为与key对应value在vLOG中offset
        vlog->Add(key, iter->value());
      }
    } else {
      for (auto& [k, v] : lists) {
        builder->Add(GetLengthPrefixedSlice(k.data()), v);
      }
      key = GetLengthPrefixedSlice(lists.rbegin()->first.data());
    }
    if (!key.empty()) {
      meta->largest.DecodeFrom(key);
    }

    // Finish and check for builder errors
    s = builder->Finish();
    if (s.ok()) {
      meta->file_size = builder->FileSize();
      assert(meta->file_size > 0);
    }
    delete builder;

    // Finish and check for file errors
    if (s.ok()) {
      s = file->Sync();
    }
    if (s.ok()) {
      s = file->Close();
    }

    if (s.ok()) {
      if (compact_memtable) {
        s = vlog->Finish();
      }
    }

    delete file;
    file = nullptr;

    if (s.ok()) {
      // Verify that the table is usable
      Iterator* it = table_cache->NewIterator(ReadOptions(), meta->number,
                                              meta->file_size);
      s = it->status();
      delete it;
    }
  }

  // Check for input iterator errors

  if (compact_memtable) {
    if (!iter->status().ok()) {
      s = iter->status();
    }
  }

  if (s.ok() && meta->file_size > 0) {
    // Keep it
  } else {
    env->RemoveFile(fname);
  }
  return s;
}

}  // namespace leveldb
