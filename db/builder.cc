// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "db/builder.h"

#include "db/dbformat.h"
#include "db/filename.h"
#include "db/version_edit.h"
#include <iostream>

#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "leveldb/table_builder.h"

#include "blob/basic_cache.h"
#include "blob/blob_builder.h"

namespace leveldb {

Status FlushBuilderAndRecordState(BlobWapper* blob_wapper,
                                  SSTWapper* sst_wapper) {
  Status s;
  if (blob_wapper) {
    s = blob_wapper->blob->Finish();
    if (s.ok()) {
      blob_wapper->meta->file_size = blob_wapper->blob->FileSize();
      assert(blob_wapper->meta->file_size > 0);
    }
    delete blob_wapper->blob;

    if (s.ok()) {
      s = blob_wapper->file->Sync();
    }

    if (s.ok()) {
      s = blob_wapper->file->Close();
    }

    delete blob_wapper->file;
    blob_wapper->file = nullptr;

  } else if (sst_wapper) {
    s = sst_wapper->table->Finish();
    if (s.ok()) {
      sst_wapper->meta->file_size = sst_wapper->table->FileSize();
      assert(sst_wapper->meta->file_size > 0);
    }
    delete sst_wapper->table;

    if (s.ok()) {
      s = sst_wapper->file->Sync();
    }
    if (s.ok()) {
      s = sst_wapper->file->Close();
    }

    delete sst_wapper->file;
    sst_wapper->file = nullptr;

  } else {
    return Status::Corruption("BlobWapper And SSTWapper are nullptr");
  }
  return s;
}

Status BuildTable(const std::string& dbname, Env* env, const Options& options,
                  BasicCache* table_cache, Iterator* iter, FileMetaData* meta,
                  BlobFileMetaData* blob_meta) {
  Status s;
  meta->file_size = 0;
  blob_meta->file_size = 0;

  iter->SeekToFirst();

  std::string sst_fname = TableFileName(dbname, meta->number);
  std::string blob_fname = BlobFileName(dbname, blob_meta->number);

  if (iter->Valid()) {
    WritableFile* sst_file = nullptr;
    WritableFile* blob_file = nullptr;
    s = env->NewWritableFile(sst_fname, &sst_file);
    if (!s.ok()) {
      return s;
    }

    s = env->NewWritableFile(blob_fname, &blob_file);
    if (!s.ok()) {
      return s;
    }

    TableBuilder* builder = new TableBuilder(options, sst_file);
    BlobBuilder* blob_builder = new BlobBuilder(options, blob_file);
    meta->smallest.DecodeFrom(iter->key());

    Slice key;

    for (; iter->Valid(); iter->Next()) {
      std::string value;
      key = iter->key();
      PutFixed64(&value, blob_builder->CurrentSizeEstimate());
      builder->Add(key, value);
      blob_builder->Add(key, iter->value());
    }

    if (!key.empty()) {
      meta->largest.DecodeFrom(key);
    }

    BlobWapper blob_wapper(blob_builder, blob_file, blob_meta);

    FlushBuilderAndRecordState(&blob_wapper, nullptr);

    builder->SetBlobNumber(blob_meta->number);
    builder->SetBlobSize(blob_meta->file_size);

    SSTWapper sst_wapper(builder, sst_file, meta);

    FlushBuilderAndRecordState(nullptr, &sst_wapper);

    if (s.ok()) {
      // Verify that the table is usable
      Iterator* it = table_cache->NewIterator(ReadOptions(), meta->number,
                                              meta->file_size);
      s = it->status();
      delete it;
    }

    // Check for input iterator errors

    if (!iter->status().ok()) {
      s = iter->status();
    }
  }

  if (s.ok() && meta->file_size > 0 && blob_meta->file_size > 0) {
    // Keep it
  } else {
    env->RemoveFile(sst_fname);
    env->RemoveFile(blob_fname);
  }
  return s;
}

}  // namespace leveldb
