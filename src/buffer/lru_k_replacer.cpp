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

auto LRUKReplacer::FindLessK(frame_id_t *id) -> bool {
  frame_id_t t = 0;
  bool found = false;
  size_t earliest_timestamp = current_timestamp_;
  for (const auto &p : record_list_) {
    if (p.evictable_ && p.que_.size() < k_) {  // find inf
      if (earliest_timestamp > p.que_.front()) {
        earliest_timestamp = p.que_.front();
        t = p.frame_id_;
        found = true;
      }
    }
  }
  if (found) {
    *id = t;
    return true;
  }
  return false;
}
auto LRUKReplacer::FindEqualK(frame_id_t *id) -> bool {
  frame_id_t t = 0;
  bool found = false;
  size_t earliest_timestamp = current_timestamp_;
  for (const auto &p : record_list_) {
    if (p.evictable_ && p.que_.size() == k_) {
      if (earliest_timestamp > p.que_.front()) {  // 找到第k次访问距离现在最远的
        earliest_timestamp = p.que_.front();
        t = p.frame_id_;
        found = true;
      }
    }
  }
  if (found) {
    *id = t;
    return true;
  }
  return false;
}
// 先找 访问小于k次的frame, 找到最早访问的那个
// 再找 访问等于k次的frame

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lk(latch_);
  ++current_timestamp_;
  frame_id_t id;
  if (FindLessK(&id) || FindEqualK(&id)) {
    *frame_id = id;
    auto p = mp_[id];
    record_list_.erase(p);
    curr_size_--;
    mp_.erase(id);
    return true;
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lk(latch_);
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
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lk(latch_);
  BUSTUB_ASSERT(frame_id < static_cast<int32_t>(replacer_size_), "frame_id should be less than replacer_size_");
  if (mp_.count(frame_id) == 0) {  // 还没有记录
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
}

void LRUKReplacer::Remove(frame_id_t frame_id) {  // O(1)
  std::lock_guard<std::mutex> lk(latch_);
  BUSTUB_ASSERT(frame_id < static_cast<int32_t>(replacer_size_), "frame_id should be less than replacer_size_");
  if (mp_.count(frame_id) == 0U) {
    return;
  }
  auto p = mp_[frame_id];
  BUSTUB_ASSERT(p->evictable_, "removed frame should be evictable");
  record_list_.erase(p);
  curr_size_--;
  mp_.erase(frame_id);
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lk(latch_);
  return curr_size_;
}

}  // namespace bustub
