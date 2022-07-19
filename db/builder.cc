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

#include "ac-key/arc_cache.h"
#include "blob/basic_cache.h"
#include "blob/blob_builder.h"

namespace leveldb {

Status FlushBuilderAndRecordState(BlobWapper* blob_wapper,
                                  SSTWapper* sst_wapper) {
  Status s;
  if (blob_wapper) {
    if (blob_wapper->blob->FileSize() != kInValidBlobFileSize) {
      s = blob_wapper->blob->Finish();
      if (s.ok()) {
        blob_wapper->meta->file_size = blob_wapper->blob->FileSize();
        assert(blob_wapper->meta->file_size > 0);
      }

      if (s.ok()) {
        s = blob_wapper->file->Sync();
      }

      if (s.ok()) {
        s = blob_wapper->file->Close();
      }
    }
    delete blob_wapper->blob;
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

void UpdateAdaptiveCache(const Slice& key, const Slice& value, Cache** cache,
                         const KPValue& kpvalue) {
  Cache::Handle* handle = (*cache)->Lookup(key);
  LRUHandle* lru_handle = reinterpret_cast<LRUHandle*>(handle);

  if (handle != nullptr) {
    HandleType handle_type = lru_handle->handle_type;
    size_t handle_size = key.size();
    std::string* new_value = nullptr;

    if (lru_handle->handle_type == HandleType::kKPHandle) {
      std::string temp;
      EncodeKPValue(&temp, kpvalue.blob_number, kpvalue.blob_size,
                    kpvalue.blob_offset);
      new_value = new std::string(temp);
      handle_size += temp.size();
    } else if (lru_handle->handle_type == HandleType::kKVHandle) {
      new_value = new std::string(value.ToString());
    }

    double caching_factor = CalculateCachingFactor(0, handle_type, handle_size);

    (*cache)->Release(handle);
    handle = (*cache)->Insert(key, reinterpret_cast<void*>(new_value), 1,
                              DeleteHandle, handle_type, caching_factor);

    (*cache)->Release(handle);
  }
}

Status BuildTable(const std::string& dbname, Env* env, const Options& options,
                  TableCache* table_cache, Iterator* iter, FileMetaData* meta,
                  BlobFileMetaData* blob_meta, Cache** adaptive_cache) {
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
    Slice value;

    for (; iter->Valid(); iter->Next()) {
      key = iter->key();
      value = iter->value();
      std::string encode_value;
      std::string encode_offset;
      bool is_separation = false;

      uint64_t offset = blob_builder->CurrentSizeEstimate();

      if (value.size() >= options.value_separation_threshold) {
        PutFixed64(&encode_offset, offset);

        encode_value.push_back(KVSeparation::kSeparation);
        encode_value += encode_offset;
        is_separation = true;
        assert(encode_value.size() == 9);

      } else {
        encode_value.push_back(KVSeparation::kNoSeparation);
        encode_value += value.ToString();
      }
      builder->Add(key, encode_value);
      if (is_separation) {
        blob_builder->Add(key, value);
      }
      UpdateAdaptiveCache(
          key, value, adaptive_cache,
          KPValue(blob_meta->number, blob_meta->file_size, offset));
    }

    if (!key.empty()) {
      meta->largest.DecodeFrom(key);
    }

    BlobWapper blob_wapper(blob_builder, blob_file, blob_meta);

    FlushBuilderAndRecordState(&blob_wapper, nullptr);

    if (blob_builder->FileSize() != kInValidBlobFileSize) {
      builder->SetBlobNumber(blob_meta->number);
      builder->SetBlobSize(blob_meta->file_size);
    }

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
  } else if (s.ok()) {
    if (meta->file_size == 0) {
      env->RemoveFile(sst_fname);
    }
    if (blob_meta->file_size == 0) {
      env->RemoveFile(blob_fname);
    }
  } else {
    env->RemoveFile(sst_fname);
    env->RemoveFile(blob_fname);
  }
  return s;
}

}  // namespace leveldb
