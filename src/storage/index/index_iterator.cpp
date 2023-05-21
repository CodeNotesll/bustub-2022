/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *buffer_pool_manager, page_id_t leaf_id, int index, int size,
                                  page_id_t next_id)
    : buffer_pool_manager_(buffer_pool_manager), leaf_id_(leaf_id), index_(index), size_(size), next_id_(next_id) {}

// INDEX_TEMPLATE_ARGUMENTS
// INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return leaf_id_ == INVALID_PAGE_ID; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  Page *page = buffer_pool_manager_->FetchPage(leaf_id_);
  page->RLatch();
  auto *leaf_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
  const MappingType &ret = leaf_node->At(index_);
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(leaf_id_, false);  // 返回引用 这里提前换出了，如何处理。。。。
  return ret;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  index_++;               // 指向下一个k/v
  if (index_ == size_) {  // 已经指向末尾
    leaf_id_ = next_id_;
    // 取出下一节点，获取size_
    if (leaf_id_ != INVALID_PAGE_ID) {
      Page *page = buffer_pool_manager_->FetchPage(leaf_id_);
      page->RLatch();  // 这里上锁失败即报错 **********************
      auto *leafpage = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
      size_ = leafpage->GetSize();
      next_id_ = leafpage->GetNextPageId();
      index_ = 0;
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(leaf_id_, false);
    } else {
      buffer_pool_manager_ = nullptr;
      index_ = -1;
      size_ = -1;
    }
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
