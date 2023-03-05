#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsSafe(BPlusTreePage *tree_node, OpType type) -> bool {
  if (type == OpType::READ) {  // read operation is always safe
    return true;
  }
  int size = tree_node->GetSize();
  int max_size = tree_node->GetMaxSize();
  int min_size = tree_node->GetMinSize();
  if (type == OpType::DELETE) {  // 删除 大于最小值
    return size > min_size;
  }
  // insert
  if (tree_node->IsLeafPage()) {
    return size < max_size - 1;
  }
  return size < max_size;
}

INDEX_TEMPLATE_ARGUMENTS  // release ancestor page latch
    void
    BPLUSTREE_TYPE::ReleasePageLatch(Transaction *transaction, OpType type) {
  std::shared_ptr<std::deque<Page *>> ptr = transaction->GetPageSet();
  if (ptr->empty()) {
    return;
  }
  Page *page;
  BPlusTreePage *tree_node;
  Page *virtual_page = reinterpret_cast<Page *>(&root_page_id_);
  while (!ptr->empty()) {
    page = ptr->front();
    ptr->pop_front();  // 出队

    if (page == virtual_page) {
      root_id_latch_.unlock();
      continue;
    }

    tree_node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    page_id_t page_id = tree_node->GetPageId();
    if (type == OpType::READ) {  // 根据操作类型，解不同的锁
      page->RUnlatch();
    } else {
      page->WUnlatch();
    }
    buffer_pool_manager_->UnpinPage(page_id, false);  // ????????????????
  }
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetLeafPage(const KeyType &key, Transaction *transaction, OpType type) -> Page * {
  if (type == OpType::READ) {
    root_id_latch_.lock();
    Page *parent_page;
    Page *page;  // 当前查询的页
    page_id_t parent_id;
    page_id_t cur_page_id = root_page_id_;
    page = buffer_pool_manager_->FetchPage(cur_page_id);

    page->RLatch();
    root_id_latch_.unlock();
    auto *bplustreepage = reinterpret_cast<BPlusTreePage *>(page->GetData());
    while (!bplustreepage->IsLeafPage()) {
      parent_page = page;
      parent_id = cur_page_id;

      auto *internalpage = reinterpret_cast<InternalPage *>(bplustreepage);
      int size = internalpage->GetSize();  // 获得array_数组大小

      int i = 1;  // 使用二分优化
      cur_page_id = internalpage->ValueAt(size - 1);
      for (i = 1; i < size; ++i) {  // 找到数组第一个>= key的k
        KeyType keyat = internalpage->KeyAt(i);
        int comparesult = comparator_(keyat, key);
        if (comparesult == -1) {  // keyat < key
          continue;
        }
        if (comparesult == 0) {  // keyat = key
          cur_page_id = internalpage->ValueAt(i);
        } else {  // compresult = 1 keyat > key
          cur_page_id = internalpage->ValueAt(i - 1);
        }
        break;
      }
      page = buffer_pool_manager_->FetchPage(cur_page_id);  // 取出下一页
      page->RLatch();

      parent_page->RUnlatch();  // 释放上一页的锁
      buffer_pool_manager_->UnpinPage(parent_id, false);

      bplustreepage = reinterpret_cast<BPlusTreePage *>(page->GetData());
    }
    return page;
  }  // delete or insert
  root_id_latch_.lock();
  Page *virtual_page = reinterpret_cast<Page *>(&root_page_id_);
  transaction->AddIntoPageSet(virtual_page);

  page_id_t cur_page_id = root_page_id_;
  Page *page = buffer_pool_manager_->FetchPage(cur_page_id);
  page->WLatch();
  // page指向frame中的一页，有is_dirty_，pin_count_, frame_id_ metadata,还有data 4kB的disk
  auto *bplustree_node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  if (IsSafe(bplustree_node, type)) {  // 判断当前节点是否安全
    ReleasePageLatch(transaction, type);
  }
  transaction->AddIntoPageSet(page);  // 把节点页放入pageset中

  // 获取实际的磁盘page
  while (!bplustree_node->IsLeafPage()) {  // 判断磁盘page是否是叶子节点, 不是叶子节点
    auto *internal_node = reinterpret_cast<InternalPage *>(bplustree_node);
    int size = internal_node->GetSize();  // 获得array_数组大小
    // parent_page_id = cur_page_id;
    int i = 1;  // 使用二分优化
    cur_page_id = internal_node->ValueAt(size - 1);
    for (i = 1; i < size; ++i) {  // 找到数组第一个>= key的k
      KeyType keyat = internal_node->KeyAt(i);
      int comparesult = comparator_(keyat, key);
      if (comparesult == -1) {  // keyat < key
        continue;
      }
      if (comparesult == 0) {  // keyat = key
        cur_page_id = internal_node->ValueAt(i);
      } else {  // compresult = 1 keyat > key
        cur_page_id = internal_node->ValueAt(i - 1);
      }
      break;
    }
    // buffer_pool_manager_->UnpinPage(parent_page_id, false);  //  先不用 unpin
    page = buffer_pool_manager_->FetchPage(cur_page_id);  // 取出下一页
    page->WLatch();
    bplustree_node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    if (IsSafe(bplustree_node, type)) {
      ReleasePageLatch(transaction, type);
    }
    transaction->AddIntoPageSet(page);
  }
  return page;
}  // getleafpage返回后，transaction中应该只有 leafpage以及因为leafpage不安全而锁住的祖先page

/**
 * Return the only value that associated with input key
 * This method is used for point query
 * @return: true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  root_id_latch_.lock();
  if (IsEmpty()) {
    root_id_latch_.unlock();
    return false;
  }
  // 获得叶子节点编号，
  root_id_latch_.unlock();  // 同样是释放root_id_的锁
  Page *page = GetLeafPage(key, transaction, OpType::READ);
  auto *leafpage = reinterpret_cast<LeafPage *>(page->GetData());
  page_id_t leaf_page_id = leafpage->GetPageId();
  int size = leafpage->GetSize();  // 获得array_数组大小
  for (int i = 0; i < size; ++i) {
    if (comparator_(leafpage->KeyAt(i), key) == 0) {
      result->emplace_back(leafpage->ValueAt(i));
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(leaf_page_id, false);  // leaf_page_ 在release中unpin
      return true;
    }
  }
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(leaf_page_id, false);
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  root_id_latch_.lock();
  LeafPage *leaf_node;
  page_id_t leaf_page_id;
  if (IsEmpty()) {
    // 申请一根节点，设置信息
    Page *page = buffer_pool_manager_->NewPage(&leaf_page_id);

    page->WLatch();  // 当前节点加锁
    // root_id_latch_.unlock(); root_id_latch_  在ReleasePageSet()解锁
    // transaction->AddIntoPageSet(reinterpret_cast<Page*>(&root_page_id_));

    leaf_node = reinterpret_cast<LeafPage *>(page->GetData());
    leaf_node->Init(leaf_page_id, INVALID_PAGE_ID, leaf_max_size_);
    leaf_node->SetNextPageId(INVALID_PAGE_ID);
    root_page_id_ = leaf_page_id;
    UpdateRootPageId(1);
    // 这里直接root_id_latch_解锁
    root_id_latch_.unlock();
    transaction->AddIntoPageSet(page);
  } else {  // find the leaf Node that should contain "key"
    root_id_latch_.unlock();
    leaf_node = reinterpret_cast<LeafPage *>(GetLeafPage(key, transaction, OpType::INSERT)->GetData());
    leaf_page_id = leaf_node->GetPageId();
  }

  int size = leaf_node->GetSize();  // 获得array_数组大小
  int max_size = leaf_node->GetMaxSize();
  for (int i = 0; i < size; ++i) {
    if (comparator_(leaf_node->KeyAt(i), key) == 0) {  // 已经存在
      ReleasePageLatch(transaction, OpType::INSERT);   // unlatch all page root_id_latch_ 怎么处理
      // 路径上从根节点到叶子节点都不安全的，每个节点都加了锁，并且root_id_latch_也加了锁
      return false;
    }
  }

  if (size < max_size - 1) {
    InsertInLeaf(leaf_node, key, value);  // 这里手动刷脏，releasepageLatch()不刷脏

    leaf_node = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(leaf_page_id)->GetData());
    buffer_pool_manager_->UnpinPage(leaf_page_id, true);  // 这里就明白UnpinPage实现 if(is_dirty) 逻辑了

    ReleasePageLatch(transaction, OpType::INSERT);
    // buffer_pool_manager_->UnpinPage(leaf_page_id, true);  // 返回前unpin
    return true;
  }
  // size == max_size - 1, split
  // create a new leaf node L'
  page_id_t right_leaf_node_id;
  // 申请一个新的叶子节点
  // ******** 可能存在一些祖先节点处于加锁并且unpin的状态，导致 NewPage返回nullptr ************************
  auto *right_leaf_node = reinterpret_cast<LeafPage *>(buffer_pool_manager_->NewPage(&right_leaf_node_id)->GetData());
  right_leaf_node->Init(right_leaf_node_id, INVALID_PAGE_ID, leaf_max_size_);
  // 临时节点
  auto *temp = new LeafPage;
  temp->Init(INVALID_PAGE_ID);
  auto src = leaf_node->GetPointer(0);
  auto dst = temp->GetPointer(0);
  // 将leafpage中元素都复制到 temp中
  memcpy(static_cast<void *>(dst), static_cast<void *>(src), size * leaf_node->GetMappingTypeSize());

  temp->SetSize(size);
  // 将 (key, value) 插入到临时节点
  InsertInLeaf(temp, key, value);  //
  // 调整两个叶子结点的 nextpageid
  right_leaf_node->SetNextPageId(leaf_node->GetNextPageId());
  leaf_node->SetNextPageId(right_leaf_node_id);
  // 分割临时节点，左右两个叶子节点的(k,v) 数对
  int left_size = (leaf_max_size_ + 1) / 2;  // 为什么使用leaf_max_size_ ?????????????????????
  int right_size = leaf_max_size_ - left_size;
  // 复制的起点
  auto src1 = temp->GetPointer(0);
  auto src2 = temp->GetPointer(left_size);
  auto leftdst = leaf_node->GetPointer(0);
  auto rightdst = right_leaf_node->GetPointer(0);  // ???????????? leafpage->rightleafpage
  // 进行复制
  std::memcpy(static_cast<void *>(leftdst), static_cast<void *>(src1), left_size * temp->GetMappingTypeSize());
  std::memcpy(static_cast<void *>(rightdst), static_cast<void *>(src2), right_size * temp->GetMappingTypeSize());
  // 设置左右两个叶子节点的大小
  leaf_node->SetSize(left_size);
  right_leaf_node->SetSize(right_size);

  // insert in parent
  delete temp;
  KeyType k = right_leaf_node->KeyAt(0);
  // transaction->GetPageSet()->pop_back();
  //  保持 InsertInParent()被调用时，transaction pageSet中 最后一个page是left_node
  InsertInParent(leaf_node, right_leaf_node, k, transaction);
  return true;
}

// left_node 没有unlatch_以及unpin, right_node是分裂得到的节点
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInParent(BPlusTreePage *left_node, BPlusTreePage *right_node, const KeyType &key,
                                    Transaction *transaction) {
  page_id_t left_node_id = left_node->GetPageId();
  page_id_t right_node_id = right_node->GetPageId();

  std::shared_ptr<std::deque<Page *>> ptr = transaction->GetPageSet();
  Page *page = ptr->back();  // 获取left_node对应在buffer中 page
  ptr->pop_back();

  if (left_node->IsRootPage()) {  // 左节点是根节点, 之前分裂的节点是根节点
    // create a new node as root
    page_id_t root_node_id;
    auto *root_node = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(&root_node_id)->GetData());
    root_node->Init(root_node_id, INVALID_PAGE_ID, internal_max_size_);
    left_node->SetParentPageId(root_node_id);  // 左右节点父节点信息改变
    right_node->SetParentPageId(root_node_id);
    root_page_id_ = root_node_id;
    UpdateRootPageId(0);  // 更新根节点编号
    root_node->SetValueAt(0, left_node_id);
    root_node->SetKeyAt(1, key);
    root_node->SetValueAt(1, right_node_id);
    root_node->SetSize(2);

    page->WUnlatch();  // page unlock   被分裂的节点是根节点，那么virtal_page还持有
    ReleasePageLatch(transaction, OpType::INSERT);

    buffer_pool_manager_->UnpinPage(root_node_id, true);   // unpin the new root node
    buffer_pool_manager_->UnpinPage(left_node_id, true);   // unpin the child node for the change of parent id
    buffer_pool_manager_->UnpinPage(right_node_id, true);  // unpin the child node for the change of parent id
    return;
  }

  // 之后left_node不用改变数据，提前unlock, unpin
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(left_node_id, true);

  // 内部节点的array_类型是KeyType, page_id_t, 和叶子节点不同
  // auto *parent_node = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_id)->GetData());
  // 从transaction 中获得parent_node
  page = ptr->back();  // 现在page指向父亲节点在buffer中frame
  auto *parent_node = reinterpret_cast<InternalPage *>(page->GetData());
  page_id_t parent_id = parent_node->GetPageId();
  int size = parent_node->GetSize();
  int maxsize = parent_node->GetMaxSize();

  // 找到父节点中指向leftpage的(key, page_id)对,
  int index = 0;
  for (int i = size - 1; i >= 0; --i) {  //
    if (parent_node->ValueAt(i) == left_node_id) {
      index = i + 1;  // 在父节点中找到指向leftpage的位置
      break;          // 在之后插入指向rightpage的位置
    }
  }

  if (size < maxsize) {  // 父节点中有足够空间, 父节点是安全的
    auto src = parent_node->GetPointer(index);
    auto dst = parent_node->GetPointer(index + 1);  // 右移
    memmove(static_cast<void *>(dst), static_cast<void *>(src), (size - index) * parent_node->GetMappingTypeSize());
    parent_node->SetKeyAt(index, key);
    parent_node->SetValueAt(index, right_node_id);
    parent_node->SetSize(size + 1);
    right_node->SetParentPageId(parent_id);  //

    page->WUnlatch();                                  // unpin,unlatch父亲节点
    buffer_pool_manager_->UnpinPage(parent_id, true);  // unpin parent node
    // buffer_pool_manager_->UnpinPage(left_node_id, true);  在之前已经unpin
    buffer_pool_manager_->UnpinPage(right_node_id, true);  // unpin the child node for the change of parent id
    ptr->pop_back();
    return;
  }

  //  size == maxsize
  //  先复制到新的区域
  using internalpair = std::pair<KeyType, page_id_t>;
  std::pair<KeyType, page_id_t> temp[size + 1];
  std::memcpy(static_cast<void *>(temp), static_cast<void *>(parent_node->GetPointer(0)),
              size * parent_node->GetMappingTypeSize());
  std::memmove(static_cast<void *>(&temp[index + 1]), static_cast<void *>(&temp[index]),
               (size - index) * sizeof(internalpair));  // size ????

  temp[index].first = key;
  temp[index].second = right_node_id;  // rightpage 是分裂产生的新节点

  page_id_t right_parent_id;
  auto *right_parent_node =
      reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(&right_parent_id)->GetData());
  right_parent_node->Init(right_parent_id, INVALID_PAGE_ID, internal_max_size_);
  // parent节点要分裂, 申请新的节点
  auto left_dst = parent_node->GetPointer(0);
  auto right_dst = right_parent_node->GetPointer(0);
  int left_size = (size + 2) / 2;  // 一定成立吗 temp中有size+1个（k/v)
  int right_size = size + 1 - left_size;
  std::memcpy(static_cast<void *>(left_dst), static_cast<void *>(temp), left_size * sizeof(internalpair));
  std::memcpy(static_cast<void *>(right_dst), static_cast<void *>(&temp[left_size]), right_size * sizeof(internalpair));

  if (index >= left_size) {  // 指向rightpage 的 k移到rightparent
    right_node->SetParentPageId(right_parent_id);
  } else {
    right_node->SetParentPageId(parent_id);
  }
  parent_node->SetSize(left_size);
  right_parent_node->SetSize(right_size);  // 分裂之后，设置左右两个父节点的size
  // buffer_pool_manager_->UnpinPage(left_node_id, true); 已经 unpin
  buffer_pool_manager_->UnpinPage(right_node_id, true);  // unpin the child node for the change of parent id
  // 提前把之后不需要的 leftpage, rightpage unpin
  // 考虑buffer size 为5，header node , parent node, right parent, left node, right node 都在buffer中
  // 之后fetch返回错误，所以尽可能提前unpin
  // *************** 取child_node是否要加锁 **********************************
  for (int i = 0; i < right_size; ++i) {  // 改变由rightparent指向的child_node的父节点
    if (i + left_size == index) {         // 此时这里指向right_node, right_node已经设置好parentId
      continue;
    }
    // 改变子节点的parentPageid 不用加锁？？？？？？？？？？？？？？？？？？？？？？
    page_id_t child_node_id = right_parent_node->ValueAt(i);
    auto *child_node = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(child_node_id)->GetData());
    child_node->SetParentPageId(right_parent_id);  // 指向新的节点
    buffer_pool_manager_->UnpinPage(child_node_id, true);
  }

  KeyType k = temp[left_size].first;
  InsertInParent(parent_node, right_parent_node, k, transaction);
}
/****************************************************/
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInLeaf(LeafPage *leaf_node, const KeyType &key, const ValueType &value) {
  int size = leaf_node->GetSize();
  int index = 0;
  // 把key插入有序位置， 找到最大的 k <= key   // 二分查找？？？？？？？？？
  for (int i = size - 1; i >= 0; --i) {  // 倒序查找第一个
    KeyType k = leaf_node->KeyAt(i);
    if (comparator_(k, key) <= 0) {  // k <= key
      index = i + 1;
      break;
    }
  }
  // index 表示放入的位置
  auto src = leaf_node->GetPointer(index);
  auto dst = leaf_node->GetPointer(index + 1);
  memmove(static_cast<void *>(dst), static_cast<void *>(src),
          (size - index) * leaf_node->GetMappingTypeSize());  // move right
  leaf_node->SetKeyAt(index, key);
  leaf_node->SetValueAt(index, value);
  leaf_node->SetSize(size + 1);
}
/*****************************************************************************
 * REMOVE
 *****************************************************************************/

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  root_id_latch_.lock();
  if (IsEmpty()) {
    root_id_latch_.unlock();
    return;
  }
  root_id_latch_.unlock();
  auto *node = reinterpret_cast<BPlusTreePage *>(GetLeafPage(key, transaction, OpType::DELETE)->GetData());
  // 在调用 DeleteEnty时，transaction的pageset中保存，叶子节点以及祖先节点在buffer中frame
  DeleteEntry(node, key, transaction);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DeleteEntry(BPlusTreePage *node, const KeyType &key, Transaction *transaction) {
  int node_size = node->GetSize();

  if (node->IsLeafPage()) {
    auto *leaf_node = reinterpret_cast<LeafPage *>(node);
    for (int i = 0; i < node_size; ++i) {
      if (comparator_(leaf_node->KeyAt(i), key) == 0) {  // i 就是要被删除的位置
        memmove(static_cast<void *>(leaf_node->GetPointer(i)), static_cast<void *>(leaf_node->GetPointer(i + 1)),
                (node_size - i - 1) * leaf_node->GetMappingTypeSize());
        leaf_node->SetSize(node_size - 1);
        break;
      }
    }
  } else {
    auto *internal_node = reinterpret_cast<InternalPage *>(node);
    for (int i = 1; i < node_size; ++i) {
      if (comparator_(internal_node->KeyAt(i), key) == 0) {
        memmove(static_cast<void *>(internal_node->GetPointer(i)),
                static_cast<void *>(internal_node->GetPointer(i + 1)),
                (node_size - i - 1) * internal_node->GetMappingTypeSize());
        internal_node->SetSize(node_size - 1);
        break;
      }
    }
  }
  // 如何判断是 rootpage() 如果只有一个节点 ？？？？？？

  node_size = node->GetSize();
  int max_size = node->GetMaxSize();
  if (node->IsLeafPage()) {
    max_size--;
  }
  int min_size = node->GetMinSize();

  if (node_size >= min_size) {  // 如果满足了下限 当前节点是安全的
    page_id_t node_id = node->GetPageId();
    buffer_pool_manager_->FetchPage(node_id);
    buffer_pool_manager_->UnpinPage(node_id, true);  // 手动刷脏
    ReleasePageLatch(transaction, OpType::DELETE);   //********* 释放当前节点以及祖先锁
    return;
  }
  // 问题是如果b+树中只有一个叶子节点，这个叶子节点也是根节点，size < 2，显然不能删除节点
  // node_size < min_size
  if (node->IsRootPage()) {    // only one pointer left; size == 1 || size == 0
    if (node->IsLeafPage()) {  // 也是叶子节点
      if (node_size == 0) {    // 唯一的节点中没有元素了
        page_id_t old_root_id = root_page_id_;
        root_page_id_ = INVALID_PAGE_ID;
        UpdateRootPageId(0);  // 更新根节点编号

        ReleasePageLatch(transaction, OpType::DELETE);   // 给root_id_latch_以及根节点解锁
        buffer_pool_manager_->DeletePage(old_root_id);   // 删除这一页, 清空B+树
      } else {                                           // size == 1, unpin &  返回
        buffer_pool_manager_->FetchPage(root_page_id_);  // 手动刷脏
        buffer_pool_manager_->UnpinPage(root_page_id_, true);
        ReleasePageLatch(transaction, OpType::DELETE);
      }
    } else {  // internal_node  size = 1
      auto *root_node = reinterpret_cast<InternalPage *>(node);
      page_id_t child_node_id = root_node->ValueAt(0);
      root_page_id_ = child_node_id;
      UpdateRootPageId(0);  // 更新根节点编号

      auto *child_node = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(child_node_id)->GetData());
      child_node->SetParentPageId(INVALID_PAGE_ID);  // 更新孩子节点的父亲节点信息
      buffer_pool_manager_->UnpinPage(child_node_id, true);

      page_id_t root_node_id = root_node->GetPageId();
      ReleasePageLatch(transaction, OpType::DELETE);  // unpin unlatch 根节点以及root_id_latch_
      // buffer_pool_manager_->UnpinPage(root_node_id, true);
      buffer_pool_manager_->DeletePage(root_node_id);  // 删除这一页
    }
    return;
  }

  std::shared_ptr<std::deque<Page *>> ptr = transaction->GetPageSet();
  Page *page = ptr->back();  // page指向node在buffer中frame
  ptr->pop_back();           // 删除即可

  // 不是 root 并且 page->GetSize() < page->GetMinSize()
  // ************************** merge or redistribute ********************************
  // find siblings
  // 这里的原则是优先找左边的 sibling,
  page_id_t node_id = node->GetPageId();
  page_id_t sibling_node_id = INVALID_PAGE_ID;
  KeyType k;         // parent 中指向sibling 或者page 的k
  int k_index = 0;   // 记住k在parent中的下标
  bool flag = true;  // flag 为真，表示sibling_child 在左边

  auto *parent = reinterpret_cast<InternalPage *>(ptr->back()->GetData());
  page_id_t parent_id = parent->GetPageId();

  for (int i = 0; i < parent->GetSize(); ++i) {
    if (parent->ValueAt(i) == node_id) {
      if (i == 0) {  // page 是parent的最左边孩子
        sibling_node_id = parent->ValueAt(1);
        k = parent->KeyAt(1);  // k 指向sibling_page
        k_index = 1;
        flag = false;
      } else {
        sibling_node_id = parent->ValueAt(i - 1);
        k = parent->KeyAt(i);  // k 指向 page
        k_index = i;
      }
      break;
    }
  }

  // ******** 可能存在一些祖先节点处于加锁并且unpin的状态，导致 FetchPage返回nullptr ************************
  Page *sibling_page = buffer_pool_manager_->FetchPage(sibling_node_id);
  sibling_page->WLatch();  // 相邻节点上锁
  auto *sibling_node = reinterpret_cast<BPlusTreePage *>(sibling_page->GetData());
  int sibling_node_size = sibling_node->GetSize();

  // ************** 合并 ***************
  if (sibling_node_size + node_size <= max_size) {
    if (!flag) {  // flag 为假， node is predecessor of sibling node
      BPlusTreePage *temp = sibling_node;
      sibling_node = node;
      node = temp;
      Page *p = sibling_page;
      sibling_page = page;
      page = p;
    }
    // 找出page_id
    sibling_node_id = sibling_node->GetPageId();
    node_id = node->GetPageId();

    sibling_node_size = sibling_node->GetSize();
    node_size = node->GetSize();

    // 现在sibling node 在 node 左边, 并且sibling_page, page指向buffer中位置
    if (node->IsLeafPage()) {  // **************** 叶子节点合并 ****************
      auto *leaf_sibling_node = reinterpret_cast<LeafPage *>(sibling_node);
      auto *leaf_node = reinterpret_cast<LeafPage *>(node);
      // 将leaf_page 中 所有的k/v复制到 leaf_sbiling_node中
      auto dst = leaf_sibling_node->GetPointer(sibling_node_size);
      auto src = leaf_node->GetPointer(0);
      memcpy(static_cast<void *>(dst), static_cast<void *>(src), node_size * leaf_node->GetMappingTypeSize());
      // 设置合并之后的节点size
      leaf_sibling_node->SetSize(sibling_node_size + node_size);
      leaf_sibling_node->SetNextPageId(leaf_node->GetNextPageId());  // 设置 next_page_id

      sibling_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(sibling_node_id, true);  // unpin 左边孩子

      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(node_id, true);
      buffer_pool_manager_->DeletePage(node_id);  // unpin 右边孩子并且删除节点
    } else {                                      // ******************** 非叶子节点合并 *****************
      auto *internal_sbiling_node = reinterpret_cast<InternalPage *>(sibling_node);
      auto *internal_node = reinterpret_cast<InternalPage *>(node);
      internal_node->SetKeyAt(0, k);
      auto dst = internal_sbiling_node->GetPointer(sibling_node_size);
      auto src = internal_node->GetPointer(0);
      memcpy(static_cast<void *>(dst), static_cast<void *>(src), node_size * internal_node->GetMappingTypeSize());
      internal_sbiling_node->SetSize(sibling_node_size + node_size);

      sibling_page->WUnlatch();                                // unlatch_
      buffer_pool_manager_->UnpinPage(sibling_node_id, true);  // unpin 左边孩子 提前unpin左边孩子

      for (int i = 0; i < node_size; ++i) {  // 合并之后改变右边page指向孩子的父指针
        page_id_t child_id = internal_node->ValueAt(i);
        auto *child = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(child_id)->GetData());
        child->SetParentPageId(sibling_node_id);
        buffer_pool_manager_->UnpinPage(child_id, true);
      }

      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(node_id, true);
      buffer_pool_manager_->DeletePage(node_id);  // unpin 右边孩子并且删除节点
    }

    // 继续在父节点删除指向右边孩子的k
    DeleteEntry(parent, k, transaction);
    return;
  }

  // sibling_node_size + node_size > max_size
  //****************************** 重分配 ********************************
  // page中节点少一个，要从sibling_node 中取出一个
  // 重新分配之后，
  if (flag) {                  // flag为真，sibling_node 在左边 （sibling_node , page)
    if (node->IsLeafPage()) {  // ***********叶子节点重分配***************
      auto *leaf_sbiling_node = reinterpret_cast<LeafPage *>(sibling_node);
      auto *leaf_node = reinterpret_cast<LeafPage *>(node);
      // 删除左孩子中最后一个（last_k,last_v)
      KeyType last_k = leaf_sbiling_node->KeyAt(sibling_node_size - 1);
      ValueType last_v = leaf_sbiling_node->ValueAt(sibling_node_size - 1);
      leaf_sbiling_node->SetSize(sibling_node_size - 1);  // 没有移动数据只是 修改大小  ????????

      sibling_page->WUnlatch();  // 提前给相邻节点解锁
      buffer_pool_manager_->UnpinPage(sibling_node_id, true);

      // 右孩子中(k,v) 右移
      auto src = leaf_node->GetPointer(0);
      auto dst = leaf_node->GetPointer(1);
      memmove(static_cast<void *>(dst), static_cast<void *>(src), node_size * leaf_node->GetMappingTypeSize());
      // 将(last_k, last_v) 设为右孩子第一个节点
      leaf_node->SetKeyAt(0, last_k);
      leaf_node->SetValueAt(0, last_v);
      leaf_node->SetSize(node_size + 1);
      // 将parent中指向右边孩子的k 设置为last_k, 保持有序性
      parent->SetKeyAt(k_index, last_k);
      // unpin 左右孩子，以及父节点
      page->WUnlatch();  // 当前节点解锁
      buffer_pool_manager_->UnpinPage(node_id, true);

      buffer_pool_manager_->FetchPage(parent_id);  // parent_node手动刷脏
      buffer_pool_manager_->UnpinPage(parent_id, true);

      ReleasePageLatch(transaction, OpType::DELETE);  // 所有祖先节点解锁
    } else {  // ******************* 非叶子结点重分配**************  左->右
      auto *internal_sbiling_node = reinterpret_cast<InternalPage *>(sibling_node);  // 左边
      auto *internal_node = reinterpret_cast<InternalPage *>(node);
      // 删除左边孩子的最后一个 (last_k, last_v)
      KeyType last_k = internal_sbiling_node->KeyAt(sibling_node_size - 1);
      page_id_t last_v = internal_sbiling_node->ValueAt(sibling_node_size - 1);
      internal_sbiling_node->SetSize(sibling_node_size - 1);

      sibling_page->WUnlatch();  // 提前给相邻节点解锁
      buffer_pool_manager_->UnpinPage(sibling_node_id, true);

      // 将右边第一个key设为父节点中指向右边孩子的k, 将所有k/v右移
      internal_node->SetKeyAt(0, k);
      auto src = internal_node->GetPointer(0);
      auto dst = internal_node->GetPointer(1);
      memmove(static_cast<void *>(dst), static_cast<void *>(src), node_size * internal_node->GetMappingTypeSize());
      // 右移之后将第一个value设为做孩子最后一个value
      internal_node->SetKeyAt(0, last_k);
      internal_node->SetValueAt(0, last_v);
      internal_node->SetSize(node_size + 1);
      // 右孩子第一个k变了，修改parent中指向右孩子的k为左孩子的最后一个key
      parent->SetKeyAt(k_index, last_k);

      // 对internal节点k/v变动，注意要改变孩子节点父指针
      auto *child_node = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(last_v)->GetData());
      child_node->SetParentPageId(node_id);
      buffer_pool_manager_->UnpinPage(last_v, true);

      // unpin 左右孩子，以及父节点
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(node_id, true);

      buffer_pool_manager_->FetchPage(parent_id);  // parent节点 手动刷脏
      buffer_pool_manager_->UnpinPage(parent_id, true);

      ReleasePageLatch(transaction, OpType::DELETE);
    }

    return;
  }

  // flag 为假， page 在左边 sibling_node 在右边 从sibling_node 中取出一个(k/v)放入page
  // ***********叶子节点重分配***************  右边->左边
  if (node->IsLeafPage()) {  // 右边孩子的第一个(k,v)放在左边孩子的最后一个(k,v)
    auto *leaf_node = reinterpret_cast<LeafPage *>(node);                  // 左边
    auto *leaf_sbiling_node = reinterpret_cast<LeafPage *>(sibling_node);  // 右边
    // 删除右孩子中第一个first_k,first_v
    KeyType first_k = leaf_sbiling_node->KeyAt(0);
    ValueType first_v = leaf_sbiling_node->ValueAt(0);
    // 右孩子中(k,v)左移
    auto src = leaf_sbiling_node->GetPointer(1);
    auto dst = leaf_sbiling_node->GetPointer(0);
    memmove(static_cast<void *>(dst), static_cast<void *>(src),
            (sibling_node_size - 1) * leaf_sbiling_node->GetMappingTypeSize());

    leaf_sbiling_node->SetSize(sibling_node_size - 1);
    sibling_page->WUnlatch();  //  相邻孩子
    buffer_pool_manager_->UnpinPage(sibling_node_id, true);

    // 将(first_, first_v) 设为左孩子最后一个节点
    leaf_node->SetKeyAt(node_size, first_k);
    leaf_node->SetValueAt(node_size, first_v);
    leaf_node->SetSize(node_size + 1);
    // 将parent中指向右边孩子的k 设置为first_k, 保持有序性
    parent->SetKeyAt(k_index, leaf_sbiling_node->KeyAt(0));

    // unpin 左右两个孩子以及父亲节点
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(node_id, true);

    buffer_pool_manager_->FetchPage(parent_id);
    buffer_pool_manager_->UnpinPage(parent_id, true);

    ReleasePageLatch(transaction, OpType::DELETE);
  } else {  // ******************* 非叶子结点重分配**************
    auto *internal_node = reinterpret_cast<InternalPage *>(node);  // 左边
    auto *internal_sbiling_node = reinterpret_cast<InternalPage *>(sibling_node);
    // 删除右边孩子的第一个 (first_k, first_v)
    // KeyType first_k = internal_sbiling_node->KeyAt(0);
    page_id_t first_v = internal_sbiling_node->ValueAt(0);
    auto dst = internal_sbiling_node->GetPointer(0);
    auto src = internal_sbiling_node->GetPointer(1);
    memmove(static_cast<void *>(dst), static_cast<void *>(src),
            (sibling_node_size - 1) * internal_sbiling_node->GetMappingTypeSize());
    internal_sbiling_node->SetSize(sibling_node_size - 1);  // 右孩子元素个数少一
    sibling_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(sibling_node_id, true);

    internal_node->SetKeyAt(node_size, k);  // 将左边孩子最后一个节点的key设为父节点中指向右孩子的key
    internal_node->SetValueAt(node_size, first_v);  // value设为右孩子中第一个value
    internal_node->SetSize(node_size + 1);          //
    parent->SetKeyAt(k_index, internal_sbiling_node->KeyAt(0));

    auto *child_node = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(first_v)->GetData());
    child_node->SetParentPageId(node_id);
    buffer_pool_manager_->UnpinPage(first_v, true);

    // unpin 左右两个孩子以及父亲节点
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(node_id, true);

    buffer_pool_manager_->FetchPage(parent_id);
    buffer_pool_manager_->UnpinPage(parent_id, true);

    ReleasePageLatch(transaction, OpType::DELETE);
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  root_id_latch_.lock();
  if (IsEmpty()) {
    root_id_latch_.unlock();
    return INDEXITERATOR_TYPE();  // 为空 返回空迭代器
  }
  page_id_t parent_id = -1;      // 上一个节点id
  page_id_t id = root_page_id_;  // 下一个取出的节点id
  Page *parent_page;
  Page *page = buffer_pool_manager_->FetchPage(id);
  page->RLatch();
  root_id_latch_.unlock();  // 解开root_id的锁
  auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  while (!node->IsLeafPage()) {  // 取出的节点不是叶子节点，
    auto *internal_node = reinterpret_cast<InternalPage *>(node);
    parent_page = page;
    parent_id = id;                  // 记住当前节点id
    id = internal_node->ValueAt(0);  // 取出最左边孩子id
    page = buffer_pool_manager_->FetchPage(id);
    page->RLatch();
    parent_page->RUnlatch();                            // 已经成功fetch&lock下一节点，父节点解锁
    buffer_pool_manager_->UnpinPage(parent_id, false);  // unpin当前节点 and 取出下一节点
    node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  }
  int size = node->GetSize();
  page_id_t next_page_id = reinterpret_cast<LeafPage *>(node)->GetNextPageId();
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(id, false);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, id, 0, size, next_page_id);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  Page *page = GetLeafPage(key);
  auto *leaf_node = reinterpret_cast<LeafPage *>(page->GetData());  // 找到key所在的叶子节点
  int size = leaf_node->GetSize();                                  // 获得叶子节点信息
  page_id_t page_id = leaf_node->GetPageId();
  page_id_t next_id = leaf_node->GetNextPageId();
  for (int i = 0; i < size; ++i) {
    if (comparator_(leaf_node->KeyAt(i), key) == 0) {  // 存在该key
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page_id, false);
      return INDEXITERATOR_TYPE(buffer_pool_manager_, page_id, i, size, next_id);
    }
  }
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page_id, false);
  return INDEXITERATOR_TYPE();
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/**
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    //
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "P=" << leaf->GetPageId() << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "P=" << inner->GetPageId() << ",parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
