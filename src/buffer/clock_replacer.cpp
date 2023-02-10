//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) {
  this->nums_pages = num_pages;
  hand = 0;
  victim_num = 0;
  pinned = std::vector<int32_t>(num_pages, 1);
  ref = std::vector<int32_t>(num_pages, 0);
  victimed = std::vector<int32_t>(num_pages, 0);
}

ClockReplacer::~ClockReplacer() = default;

auto ClockReplacer::Victim(frame_id_t *frame_id) -> bool {  // buffer pool is full
  if (!victim_num) return false;
  bool found = false;
  while (!found) {
    if (!pinned[hand] && !victimed[hand]) {
      if (!ref[hand]) {
        victim_num--;
        victimed[hand] = 1;
        found = true;
        *frame_id = hand;
      } else {
        ref[hand] = 0;
      }
    }
    hand = (hand + 1) % nums_pages;
  }
  return found;
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  if (victimed[frame_id]) return;
  if (pinned[frame_id]++ == 0) victim_num--;
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  if (!pinned[frame_id]) return;
  if (--pinned[frame_id] == 0) victim_num++;
  ref[frame_id] = 1;
}

auto ClockReplacer::Size() -> size_t { return victim_num; }

}  // namespace bustub
