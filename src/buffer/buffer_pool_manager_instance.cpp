//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.push_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}
auto BufferPoolManagerInstance::GetFrameId(frame_id_t *fid) -> bool {
  frame_id_t frame_id;
  if (free_list_.empty()) {
    if (!replacer_->Evict(&frame_id)) {
      return false;
    }
    page_id_t id = pages_[frame_id].page_id_;
    if (pages_[frame_id].is_dirty_) {
      disk_manager_->WritePage(id, pages_[frame_id].data_);
    }
    pages_[frame_id].ResetMemory();
    page_table_->Remove(id);
  } else {
    frame_id = free_list_.front();
    free_list_.pop_front();
  }
  *fid = frame_id;
  return true;
}
void BufferPoolManagerInstance::Init(frame_id_t frame_id, page_id_t page_id) {
  page_table_->Insert(page_id, frame_id);
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].page_id_ = page_id;
}
auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (!GetFrameId(&frame_id)) {
    return nullptr;
  }
  page_id_t id = AllocatePage();
  *page_id = id;
  Init(frame_id, id);
  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (page_table_->Find(page_id, frame_id)) {
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    pages_[frame_id].pin_count_++;
    return &pages_[frame_id];
  }

  if (!GetFrameId(&frame_id)) {
    // std::cout << GREEEN << "fetch return nullptr" << END << std::endl;
    return nullptr;
  }
  // pages_[frame_id].ResetMemory();  // 设置初始内容
  Init(frame_id, page_id);
  disk_manager_->ReadPage(page_id, pages_[frame_id].data_);
  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  // 做两件事
  // 1. 设置标志位
  // 2. 设置pin_count_
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }
  if (is_dirty) {
    pages_[frame_id].is_dirty_ = is_dirty;
  }
  if (pages_[frame_id].pin_count_ == 0) {
    return false;
  }
  pages_[frame_id].pin_count_--;
  if (pages_[frame_id].pin_count_ == 0U) {
    replacer_->SetEvictable(frame_id, true);
  }
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  // std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {  // ?
    return false;
  }
  // BUSTUB_ASSERT(pages_[frame_id].pin_count_ == 0, "flushed page should be unpinned");
  //  page_id -> frame_id
  //  get page by frmae_id  pages_[frmae_id]
  //  1. 将页写回磁盘， diskmanager.write()
  //  2.reset 这个buffer pool页
  //  删除replacer hash表中相应的记录
  //  将frame_id放回free_list
  disk_manager_->WritePage(page_id, pages_[frame_id].data_);
  // pages_[frame_id].ResetMemory();
  pages_[frame_id].is_dirty_ = false;
  // pages_[frame_id].pin_count_ = 0;
  // pages_[frame_id].page_id_ = INVALID_PAGE_ID;

  // free_list_.insert(frame_id);
  // page_table_->Remove(page_id);
  // replacer_->Remove(frame_id);
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock<std::mutex> lock(latch_);
  for (size_t i = 0; i < pool_size_; ++i) {
    // if (free_list_.count(i) != 0) {
    //   continue;
    // }
    FlushPgImp(pages_[i].page_id_);
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return true;
  }
  if (pages_[frame_id].pin_count_ != 0) {
    return false;
  }
  // if (pages_[frame_id].is_dirty_) { // 直接删除了， 可能发生在B+节点合并时
  //   disk_manager_->WritePage(page_id, pages_[frame_id].data_);  // 写回磁盘
  // }

  pages_[frame_id].ResetMemory();
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;

  free_list_.push_back(frame_id);
  page_table_->Remove(page_id);
  replacer_->Remove(frame_id);
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
