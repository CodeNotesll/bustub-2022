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
  dir_[0] = std::make_shared<Bucket>(bucket_size_, 0);
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
  dir_[index]->Insert(key, value);
  if (!dir_[index]->Isoverflow()) {
    rwlatch_.WUnlock();
    return;
  }
  // RedistributeBucket(index);
  std::queue<size_t> que;
  que.push(index);
  while (!que.empty()) {
    std::cout << "overflow!" << std::endl;
    index = que.front();
    que.pop();
    size_t new_index = index;  // local depth equals to global depth; double directories
    if (dir_[index]->GetDepth() == global_depth_) {
      int old_len = 1 << global_depth_;
      global_depth_++;
      int new_len = 1 << global_depth_;
      dir_.resize(new_len);
      for (int i = old_len; i < new_len; ++i) {
        dir_[i] = dir_[i - old_len];
      }
      new_index = index + old_len;
    }
    // split overflowed buckets
    dir_[index]->IncrementDepth();
    auto p = dir_[index];
    dir_[new_index] = std::make_shared<Bucket>(bucket_size_, p->GetDepth());
    for (auto it = p->GetItems().begin(); it != p->GetItems().end();) {
      if (IndexOf(it->first) == new_index) {
        dir_[new_index]->GetItems().insert(dir_[new_index]->GetItems().begin(), *it);
        it = p->GetItems().erase(it);
      } else {
        it++;
      }
    }
    num_buckets_++;
    if (dir_[index]->Isoverflow()) {
      que.push(index);
    }
    if (dir_[new_index]->Isoverflow()) {
      que.push(new_index);
    }
  }
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
  for (auto &[k, v] : list_) {
    if (k == key) {
      v = value;
      return true;
    }
  }
  list_.insert(list_.begin(), {key, value});
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
