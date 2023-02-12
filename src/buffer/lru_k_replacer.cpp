//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  latch_.lock();
  size_t distance = 0;
  size_t eraliset_timestamp = ++current_timestamp_;
  bool found = false;
  frame_id_t id;
  for (const auto &p : record_list_) {
    if (!p.evictable_) {
      continue;
    }
    if (p.que_.size() < k_) {  // find inf
      if (eraliset_timestamp > p.que_.front()) {
        eraliset_timestamp = p.que_.front();
        id = p.frame_id_;
        found = true;
      }
    }
  }
  if (found) {
    *frame_id = id;
    auto p = mp_[id];
    record_list_.erase(p);
    curr_size_--;
    mp_.erase(id);
    latch_.unlock();
    return true;
  }

  for (const auto &p : record_list_) {
    if (!p.evictable_) {
      continue;
    }
    if (p.que_.size() == k_) {
      if (distance < current_timestamp_ - p.que_.front()) {
        distance = current_timestamp_ - p.que_.front();
        id = p.frame_id_;
        found = true;
      }
    }
  }
  if (found) {
    *frame_id = id;
    auto p = mp_[id];
    record_list_.erase(p);
    curr_size_--;
    mp_.erase(id);
    latch_.unlock();
    return true;
  }
  latch_.unlock();
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  latch_.lock();
  BUSTUB_ASSERT(frame_id < static_cast<int32_t>(replacer_size_), "frame_id should be less than replacer_size_");
  current_timestamp_++;
  if (mp_.count(frame_id) != 0) {
    auto p = mp_[frame_id];
    p->que_.push(current_timestamp_);
    if (p->que_.size() == k_ + 1) {
      p->que_.pop();
    }
  } else {
    Record rec(frame_id);
    rec.que_.push(current_timestamp_);
    record_list_.insert(record_list_.begin(), rec);
    mp_[frame_id] = record_list_.begin();
  }
  latch_.unlock();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  latch_.lock();
  BUSTUB_ASSERT(frame_id < static_cast<int32_t>(replacer_size_), "frame_id should be less than replacer_size_");
  if (mp_.count(frame_id) == 0) {  // 还没有记录
    latch_.unlock();
    return;
  }
  auto p = mp_[frame_id];
  if (set_evictable && !p->evictable_) {
    curr_size_++;
  }
  if (!set_evictable && p->evictable_) {
    curr_size_--;
  }
  p->evictable_ = set_evictable;
  latch_.unlock();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {  // O(1)
  latch_.lock();
  BUSTUB_ASSERT(frame_id < static_cast<int32_t>(replacer_size_), "frame_id should be less than replacer_size_");
  if (mp_.count(frame_id) == 0U) {
    latch_.unlock();
    return;
  }
  auto p = mp_[frame_id];
  BUSTUB_ASSERT(p->evictable_, "removed frame should be evictable");
  record_list_.erase(p);
  curr_size_--;
  mp_.erase(frame_id);
  latch_.unlock();
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
