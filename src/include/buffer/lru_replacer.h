//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.h
//
// Identification: src/include/buffer/lru_replacer.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include "buffer/replacer.h"
#include "common/config.h"

namespace bustub {

/**
 * LRUReplacer implements the Least Recently Used replacement policy.
 */
class LRUReplacer : public Replacer {
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  /**
   * Remove the victim frame as defined by the replacement policy.
   * @param[out] frame_id id of frame that was removed, nullptr if no victim was found
   * @brief 不会改变状态
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
  struct Doublelinklist {
    frame_id_t frame_id_;
    size_t timestamp_;
    Doublelinklist *pre_;
    Doublelinklist *next_;
    explicit Doublelinklist(frame_id_t frame_id, size_t timestamp)
        : frame_id_(frame_id), timestamp_(timestamp), pre_(nullptr), next_(nullptr) {}
  };
  using dll = Doublelinklist;
  dll *head_;
  dll *rear_;
  std::unordered_map<frame_id_t, dll *> mp_;
  std::unordered_set<frame_id_t> pinned_;
  std::unordered_set<frame_id_t> evicted_;
  size_t num_pages_;
  void Put(frame_id_t frame_id);
  void Erase(frame_id_t frame_id);
};

}  // namespace bustub
