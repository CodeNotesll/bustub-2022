//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {
  head_ = nullptr;
  rear_ = nullptr;
  for (size_t i = 0; i < num_pages; ++i) pinned_.insert(i);
  this->num_pages_ = num_pages;
}

LRUReplacer::~LRUReplacer() {
  rear_ = nullptr;
  while (head_) {
    dll *oldhead = head_;
    head_ = head_->next_;
    delete oldhead;
  }
}

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool {
  if (!rear_) return false;
  frame_id_t f = rear_->frame_id_;
  evicted_.insert(f);
  *frame_id = f;
  Erase(f);
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  if (pinned_.count(frame_id) || evicted_.count(frame_id)) return;
  pinned_.insert(frame_id);
  Erase(frame_id);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  if (!pinned_.count(frame_id)) return;
  if (evicted_.count(frame_id)) evicted_.erase(frame_id);
  pinned_.erase(frame_id);
  Put(frame_id);
}

auto LRUReplacer::Size() -> size_t { return num_pages_ - pinned_.size() - evicted_.size(); }

void LRUReplacer::Put(frame_id_t frame_id) {
  dll *oldhead = head_;
  head_ = new dll(frame_id, 1);
  mp_[frame_id] = head_;
  head_->next_ = oldhead;
  if (oldhead)
    oldhead->pre_ = head_;
  else
    rear_ = head_;
}

void LRUReplacer::Erase(frame_id_t frame_id) {
  if (!rear_ || rear_->frame_id_ != frame_id) return;
  mp_.erase(frame_id);
  dll *oldrear = rear_;
  dll *curpre = rear_->pre_;
  dll *curnext = rear_->next_;
  rear_ = curpre;
  if (curpre) {
    curpre->next_ = curnext;
  } else {
    head_ = rear_;
  }
  delete oldrear;
}
}  // namespace bustub
