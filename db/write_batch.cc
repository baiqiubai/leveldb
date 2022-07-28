// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBatch::rep_ :=
//    sequence: fixed64
//    count: fixed32
//    data: record[count]
// record :=
//    kTypeValue varstring varstring         |
//    kTypeDeletion varstring
//    kTypeGuard varstring varint32
// varstring :=
//    len: varint32
//    data: uint8[len]

#include "leveldb/write_batch.h"

#include "db/dbformat.h"
#include "db/memtable.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"

#include "leveldb/db.h"

#include "util/coding.h"
#include "util/murmurhash3.h"

namespace leveldb {

// WriteBatch header has an 8-byte sequence number followed by a 4-byte count.
static const size_t kHeader = 12;

WriteBatch::WriteBatch() { Clear(); }

WriteBatch::~WriteBatch() = default;

WriteBatch::Handler::~Handler() = default;

void WriteBatch::Clear() {
  rep_.clear();
  rep_.resize(kHeader);
}

size_t WriteBatch::ApproximateSize() const { return rep_.size(); }

Status WriteBatch::Iterate(Handler* handler) const {
  Slice input(rep_);
  if (input.size() < kHeader) {
    return Status::Corruption("malformed WriteBatch (too small)");
  }

  input.remove_prefix(kHeader);
  Slice key, value;
  uint32_t level = 0;
  int found = 0;
  while (!input.empty()) {
    found++;
    char tag = input[0];
    input.remove_prefix(1);
    switch (tag) {
      case kTypeValue:
        if (GetLengthPrefixedSlice(&input, &key) &&
            GetLengthPrefixedSlice(&input, &value)) {
          handler->Put(key, value);
        } else {
          return Status::Corruption("bad WriteBatch Put");
        }
        break;
      case kTypeDeletion:
        if (GetLengthPrefixedSlice(&input, &key)) {
          handler->Delete(key);
        } else {
          return Status::Corruption("bad WriteBatch Delete");
        }
        break;
      case kTypeGuard:
        if (GetLengthPrefixedSlice(&input, &key) &&
            GetVarint32(&input, &level)) {
          handler->InsertGuard(key, level);
        } else {
          return Status::Corruption("bad WriteBatch Guard");
        }
        break;
      default:
        return Status::Corruption("unknown WriteBatch tag");
    }
  }
  if (found != WriteBatchInternal::Count(this)) {
    return Status::Corruption("WriteBatch has wrong count");
  } else {
    return Status::OK();
  }
}

int WriteBatchInternal::Count(const WriteBatch* b) {
  return DecodeFixed32(b->rep_.data() + 8);
}

void WriteBatchInternal::SetCount(WriteBatch* b, int n) {
  EncodeFixed32(&b->rep_[8], n);
}

SequenceNumber WriteBatchInternal::Sequence(const WriteBatch* b) {
  return SequenceNumber(DecodeFixed64(b->rep_.data()));
}

void WriteBatchInternal::SetSequence(WriteBatch* b, SequenceNumber seq) {
  EncodeFixed64(&b->rep_[0], seq);
}

void WriteBatch::Put(const Slice& key, const Slice& value) {
  WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
  rep_.push_back(static_cast<char>(kTypeValue));
  PutLengthPrefixedSlice(&rep_, key);
  PutLengthPrefixedSlice(&rep_, value);
  WriteBatchInternal::SelectGuard(this, key);
}

void WriteBatch::PutGuard(const Slice& key, uint32_t level) {
  WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
  rep_.push_back(static_cast<char>(kTypeGuard));
  PutLengthPrefixedSlice(&rep_, key);
  PutVarint32(&rep_, level);
}

void WriteBatch::Delete(const Slice& key) {
  WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
  rep_.push_back(static_cast<char>(kTypeDeletion));
  PutLengthPrefixedSlice(&rep_, key);
  WriteBatchInternal::SelectGuard(this, key);
}

void WriteBatch::Append(const WriteBatch& source) {
  WriteBatchInternal::Append(this, &source);
}

namespace {
class MemTableInserter : public WriteBatch::Handler {
 public:
  SequenceNumber sequence_;
  MemTable* mem_;
  Version* version_;

  MemTableInserter(SequenceNumber sequence, MemTable* mem, Version* version)
      : sequence_(sequence), mem_(mem), version_(version) {}

  void Put(const Slice& key, const Slice& value) override {
    mem_->Add(sequence_, kTypeValue, key, value);
    sequence_++;
  }
  void Delete(const Slice& key) override {
    mem_->Add(sequence_, kTypeDeletion, key, Slice());
    sequence_++;
  }
  void InsertGuard(const Slice& key, uint32_t level) override {
    GuardMetaData* guard = new GuardMetaData();
    guard->refs = 1;
    guard->num_of_segments = 0;
    guard->level = level;
    InternalKey internal_key(key, sequence_, kTypeValue);
    guard->guard_key = internal_key;
    version_->AddUnCommittedGuard(level, guard);
    sequence_++;
  }
};
}  // namespace

Status WriteBatchInternal::InsertInto(const WriteBatch* b, MemTable* memtable) {
  return InsertIntoVersion(b, memtable, nullptr);
}

Status WriteBatchInternal::InsertIntoVersion(const WriteBatch* b,
                                             MemTable* memtable,
                                             Version* version) {
  MemTableInserter inserter(WriteBatchInternal::Sequence(b), memtable, version);
  return b->Iterate(&inserter);
}

void WriteBatchInternal::SetContents(WriteBatch* b, const Slice& contents) {
  assert(contents.size() >= kHeader);
  b->rep_.assign(contents.data(), contents.size());
}

void WriteBatchInternal::Append(WriteBatch* dst, const WriteBatch* src) {
  SetCount(dst, Count(dst) + Count(src));
  assert(src->rep_.size() >= kHeader);
  dst->rep_.append(src->rep_.data() + kHeader, src->rep_.size() - kHeader);
}

void WriteBatchInternal::SelectGuard(WriteBatch* batch, const Slice& key) {
  // Need to hash and check the last few bits.
  void* input = (void*)key.data();
  size_t size = key.size();
  const unsigned int murmur_seed = 42;
  unsigned int hash_result;
  MurmurHash3_x86_32(input, size, murmur_seed, &hash_result);

  // Go through each level, starting from the top and checking if it
  // is a guard on that level.
  unsigned bit_mask = 0;
  unsigned num_bits = top_level_bits;

  for (unsigned i = 0; i < config::kNumLevels; i++) {
    SetMask(&bit_mask, num_bits);
    if (i == 0) {
      num_bits -= bit_decrement;
      continue;
    }
    if ((hash_result & bit_mask) == bit_mask) {
      // found a guard
      // Insert the guard to this level and all the lower levels
      for (unsigned j = i; j < config::kNumLevels; j++) {
        batch->PutGuard(key, j);
      }
      break;
    }
    // Check next level
    num_bits -= bit_decrement;
  }
}

}  // namespace leveldb
