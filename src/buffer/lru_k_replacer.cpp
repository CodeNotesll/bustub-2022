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

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k)
    : replacer_size_(num_frames), k_(k), head_(nullptr), rear_(nullptr) {}

LRUKReplacer::~LRUKReplacer() {
  while (head_ != nullptr) {
    auto cur = head_;
    head_ = head_->next_;
    delete cur;
  }
}
auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  size_t cur_diff = 0;
  size_t cur_stamp = current_timestamp_;
  bool found = false;
  frame_id_t id;
  for (auto p = head_; p != nullptr; p = p->next_) {
    if (!p->evictable_) {
      continue;
    }
    if (p->diff_ > cur_diff) {
      cur_diff = p->diff_;
      id = p->frame_id_;
      found = true;
    } else if (p->diff_ == cur_diff) {
      if (p->que_.front() <= cur_stamp) {
        cur_stamp = p->que_.front();
        id = p->frame_id_;
        found = true;
      }
    }
  }
  if (!found) {
    return false;
  }
  *frame_id = id;
  Remove(id);
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  BUSTUB_ASSERT(frame_id < static_cast<int32_t>(replacer_size_), "frame_id should be less than replacer_size_");
  current_timestamp_++;
  if (mp_.count(frame_id) != 0) {
    historyNode *p = mp_[frame_id];
    p->que_.push(current_timestamp_);
    if (p->que_.size() == k_ + 1) {
      p->que_.pop();
    }
    if (p->que_.size() == k_) {
      p->diff_ = current_timestamp_ - p->que_.front();
    }
  } else {
    historyNode *oldhead = head_;
    head_ = new History(frame_id);
    head_->que_.push(current_timestamp_);
    head_->next_ = oldhead;
    if (oldhead != nullptr) {
      oldhead->pre_ = head_;
    } else {
      rear_ = head_;
    }
    mp_[frame_id] = head_;
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  BUSTUB_ASSERT(frame_id < static_cast<int32_t>(replacer_size_), "frame_id should be less than replacer_size_");
  BUSTUB_ASSERT(mp_.count(frame_id), "frame should be have been recorded");
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
  BUSTUB_ASSERT(frame_id < static_cast<int32_t>(replacer_size_), "frame_id should be less than replacer_size_");
  if (mp_.count(frame_id) == 0U) {
    return;
  }
  auto p = mp_[frame_id];
  BUSTUB_ASSERT(p->evictable_, "removed frame should be evictable");
  if (p == head_) {
    head_ = head_->next_;
    if (head_ != nullptr) {
      head_->pre_ = nullptr;
    } else {
      rear_ = head_;
    }
  } else if (p == rear_) {
    rear_ = rear_->pre_;
    if (rear_ != nullptr) {
      rear_->next_ = nullptr;
    } else {
      head_ = rear_;
    }
  } else {
    auto pre = p->pre_;
    auto next = p->next_;
    pre->next_ = next;
    next->pre_ = pre;
  }
  p->pre_ = nullptr;
  p->next_ = nullptr;
  delete p;
  curr_size_--;
  mp_.erase(frame_id);
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
