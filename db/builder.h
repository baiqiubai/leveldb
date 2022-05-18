// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_BUILDER_H_
#define STORAGE_LEVELDB_DB_BUILDER_H_

#include <memory>
#include <vector>

#include "leveldb/status.h"

namespace leveldb {

struct Options;
struct FileMetaData;
struct BlobFileMetaData;

class Env;
class Iterator;
class TableCache;
class VersionEdit;
class BlobBuilder;
class TableBuilder;
class BasicCache;
class WritableFile;
// Build a Table file from the contents of *iter.  The generated file
// will be named according to meta->number.  On success, the rest of
// *meta will be filled with metadata about the generated table.
// If no data is present in *iter, meta->file_size will be set to
// zero, and no Table file will be produced.
Status BuildTable(const std::string& dbname, Env* env, const Options& options,
                  BasicCache* table_cache, Iterator* iter, FileMetaData* meta,
                  BlobFileMetaData* blob_meta);

struct BlobWapper {
  BlobWapper(BlobBuilder* blob, WritableFile* file, BlobFileMetaData* meta)
      : blob(blob), file(file), meta(meta) {}

  ~BlobWapper() = default;

  BlobWapper(const BlobWapper&) = delete;
  BlobWapper& operator=(const BlobWapper&) = delete;

  BlobBuilder* blob;
  WritableFile* file;
  BlobFileMetaData* meta;
};
struct SSTWapper {
  SSTWapper(TableBuilder* table, WritableFile* file, FileMetaData* meta)
      : table(table), file(file), meta(meta) {}

  ~SSTWapper() = default;

  SSTWapper(const SSTWapper&) = delete;
  SSTWapper& operator=(const SSTWapper&) = delete;

  TableBuilder* table;
  WritableFile* file;
  FileMetaData* meta;
};

Status FlushBuilderAndRecordState(BlobWapper* blob_wapper,
                                  SSTWapper* sst_wapper);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_BUILDER_H_
