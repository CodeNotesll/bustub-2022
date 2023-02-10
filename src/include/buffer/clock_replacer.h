//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.h
//
// Identification: src/include/buffer/clock_replacer.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <unordered_set>
#include <vector>
#include "buffer/replacer.h"
#include "common/config.h"

namespace bustub {

/**
 * ClockReplacer implements the clock replacement policy, which approximates the Least Recently Used policy.
 */
class ClockReplacer : public Replacer {
 public:
  /**
   * Create a new ClockReplacer.
   * @param num_pages the maximum number of pages the ClockReplacer will be required to store
   */
  explicit ClockReplacer(size_t num_pages);

  /**
   * Destroys the ClockReplacer.
   */
  ~ClockReplacer() override;

  /**
   * Remove the victim frame as defined by the replacement policy.
   * @param[out] frame_id id of frame that was removed, nullptr if no victim was found
   * @return true if a victim frame was found, false otherwise
   */
  auto Victim(frame_id_t *frame_id) -> bool override;

  /**
   * Pins a frame, indicating that it should not be victimized until it is unpinned.
   * @param frame_id the id of the frame to pin
   */
  void Pin(frame_id_t frame_id) override;

  /**
   * Unpins a frame, indicating that it can now be victimized.
   * @param frame_id the id of the frame to unpin
   */
  void Unpin(frame_id_t frame_id) override;

  /** @return the number of elements in the replacer that can be victimized */
  auto Size() -> size_t override;

 private:
  // TODO(student): implement me!
  std::vector<int32_t> pinned;
  std::vector<int32_t> ref;
  std::vector<int32_t> victimed;
  size_t nums_pages;
  size_t hand;
  size_t victim_num;
};

}  // namespace bustub
