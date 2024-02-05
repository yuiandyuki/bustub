//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <functional>
#include <list>
#include <memory>
#include <utility>

#include "common/macros.h"
#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.resize(num_buckets_, std::make_shared<Bucket>(bucket_size));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLowKBit(int k, size_t n) -> int {
  return n & ((1 << k) - 1);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::TestKBit(int k, size_t n) -> int {
  return n & (1 << k);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  // UNREACHABLE("not implemented");
  std::lock_guard<std::mutex> lk(latch_);
  auto bucket_pos = IndexOf(key);
  return dir_[bucket_pos]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  // UNREACHABLE("not implemented");
  std::lock_guard<std::mutex> lk(latch_);
  auto bucket_pos = IndexOf(key);
  return dir_[bucket_pos]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  // UNREACHABLE("not implemented");
  std::lock_guard<std::mutex> lk(latch_);
  while (true) {
    auto bucket_pos = IndexOf(key);
    auto cur_bucket = dir_[bucket_pos];
    if (cur_bucket->Insert(key, value)) {
      return;
    }

    int cur_depth = cur_bucket->GetDepth();
    assert(global_depth_ >= cur_depth);
    if (global_depth_ == cur_depth) {
      size_t size = dir_.size();
      dir_.resize(2 * size);
      for (size_t i = 0; i < size; i++) {
        dir_[size + i] = dir_[i];
      }
      global_depth_++;
    }

    if (global_depth_ > cur_depth) {
      RedistributeBucket(cur_bucket);
    }
  }
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket) -> void {
  int depth = bucket->GetDepth();
  bucket->IncrementDepth();
  auto new_bucket = std::make_shared<Bucket>(bucket_size_, depth + 1);
  num_buckets_++;

  auto &cur_items = bucket->GetItems();
  int lowbit = GetLowKBit(depth, std::hash<K>()(cur_items.begin()->first));
  for (size_t i = 0; i < dir_.size(); ++i) {
    if (TestKBit(depth, i) && GetLowKBit(depth, i) == lowbit) {
      dir_[i] = new_bucket;
    }
  }

  for (auto it = cur_items.begin(); it != cur_items.end();) {
    if (TestKBit(depth, std::hash<K>()(it->first))) {
      BUSTUB_ASSERT(!new_bucket->IsFull(), "bucket is not full");
      new_bucket->Insert(it->first, it->second);
      it = cur_items.erase(it);
    } else {
      it++;
    }
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::FindKeyPos(const K &key) -> typename std::list<std::pair<K, V>>::iterator {
  auto res = list_.end();
  for (auto it = list_.begin(); it != list_.end(); it++) {
    if (it->first == key) {
      res = it;
      break;
    }
  }
  return res;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::IndexOf(const K &key) -> size_t {
  int mask = (1 << depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::CheckBucket() -> bool {
  if (list_.empty()) {
    return true;
  }
  size_t excepted = IndexOf(list_.begin()->first);
  bool res = std::all_of(list_.begin(), list_.end(), [&excepted, this](const std::pair<K, V> &p) {
    auto got = IndexOf(p.first);
    return got == excepted;
  });
  return res;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  // UNREACHABLE("not implemented");
  auto res = FindKeyPos(key);
  if (res == list_.end()) {
    return false;
  }
  value = res->second;
  return true;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  // UNREACHABLE("not implemented");
  auto del_pos = FindKeyPos(key);
  if (del_pos == list_.end()) {
    return false;
  }
  list_.erase(del_pos);
  return true;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  // UNREACHABLE("not implemented");
  auto insert_pos = FindKeyPos(key);
  if (insert_pos == list_.end()) {
    if (IsFull()) {
      return false;
    }
    list_.emplace_front(key, value);
    return true;
  }
  insert_pos->second = value;
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
