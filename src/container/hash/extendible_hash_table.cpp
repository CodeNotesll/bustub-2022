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

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {  //
  dir_.resize(1);
  dir_[0] = std::make_shared<Bucket>(bucket_size_);
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
  rwlatch_.RLock();
  size_t index = IndexOf(key);
  bool res = dir_[index]->Find(key, value);
  rwlatch_.RUnlock();
  return res;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  rwlatch_.WLock();
  size_t index = IndexOf(key);
  bool res = dir_[index]->Remove(key);
  rwlatch_.WUnlock();
  return res;
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  rwlatch_.WLock();
  size_t index = IndexOf(key);
  if (bucket_size_ == 0 || dir_[index]->Update(key, value)) {
    rwlatch_.WUnlock();
    return;
  }
  while (dir_[index]->IsFull()) {
    // size_t new_index = index;  // local depth equals to global depth; double directories
    if (dir_[index]->GetDepth() == global_depth_) {
      int old_len = dir_.size();
      global_depth_++;
      int new_len = 1 << global_depth_;
      dir_.resize(new_len);
      for (int i = old_len; i < new_len; ++i) {
        dir_[i] = dir_[i - old_len];
      }
    }
    // split overflowed buckets
    auto p = dir_[index];
    int mask = 1 << p->GetDepth();
    auto zero_bucket = std::make_shared<Bucket>(bucket_size_, p->GetDepth() + 1);
    auto one_bucket = std::make_shared<Bucket>(bucket_size_, p->GetDepth() + 1);
    for (auto it = p->GetItems().begin(); it != p->GetItems().end(); it++) {
      if ((std::hash<K>()(it->first) & mask) != 0) {
        one_bucket->GetItems().insert(one_bucket->GetItems().begin(), *it);
      } else {
        zero_bucket->GetItems().insert(zero_bucket->GetItems().begin(), *it);
      }
    }
    num_buckets_++;
    for (size_t i = 0; i < dir_.size(); ++i) {
      if (dir_[i] == p) {
        if ((i & mask) != 0) {
          dir_[i] = one_bucket;
        } else {
          dir_[i] = zero_bucket;
        }
      }
    }
    index = IndexOf(key);
  }
  index = IndexOf(key);
  dir_[index]->Insert(key, value);
  rwlatch_.WUnlock();
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::RedistributeBucket(size_t ind) {}
//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (const auto &[k, v] : list_) {
    if (k == key) {
      value = v;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for (auto it = list_.begin(); it != list_.end(); ++it) {
    if (it->first == key) {
      list_.erase(it);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  list_.insert(list_.begin(), {key, value});
  return true;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Update(const K &key, const V &value) -> bool {
  for (auto &[k, v] : list_) {
    if (k == key) {
      v = value;
      return true;
    }
  }
  return false;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
