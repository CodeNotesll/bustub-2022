//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator(BufferPoolManager *buffer_pool_manager, page_id_t leaf_id, int index, int max_size, page_id_t next_id);

  IndexIterator();

  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    return buffer_pool_manager_ == itr.buffer_pool_manager_ && leaf_id_ == itr.leaf_id_ && index_ == itr.index_;
  }

  auto operator!=(const IndexIterator &itr) const -> bool {
    return !(buffer_pool_manager_ == itr.buffer_pool_manager_ && leaf_id_ == itr.leaf_id_ && index_ == itr.index_);
  }

 private:
  // add your own private member variables here
  BufferPoolManager *buffer_pool_manager_ = nullptr;  // 使用的buffer pool manager
  page_id_t leaf_id_ = INVALID_PAGE_ID;               // 指向的叶子节点编号
  int index_ = -1;                                    // 指向的k/v在叶子节点中下标
  int size_ = -1;                                     // 记住每个leaf page k/v的数量，++之后与index_比较
  page_id_t next_id_ =
      INVALID_PAGE_ID;  // 记住每个leaf page next_id, 这样在 index_ == size_时不用取出当前页查看nextpageid
};

}  // namespace bustub
