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
auto BPLUSTREE_TYPE::GetLeafPageId(const KeyType &key) -> page_id_t {
  page_id_t parent_page_id = -1;
  page_id_t cur_page_id = root_page_id_;
  Page *page = buffer_pool_manager_->FetchPage(cur_page_id);
  // page指向frame中的一页，有is_dirty_，pin_count_, frame_id_ metadata,还有data 4kB的disk
  auto *bplustreepage = reinterpret_cast<BPlusTreePage *>(page->GetData());
  // 获取实际的磁盘page
  while (!bplustreepage->IsLeafPage()) {  // 判断磁盘page是否是叶子节点, 不是叶子节点
    auto *internalpage = reinterpret_cast<InternalPage *>(bplustreepage);
    int size = internalpage->GetSize();  // 获得array_数组大小
    parent_page_id = cur_page_id;
    int i = 1;
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
    // if (i == size) {  // 所有的keyat < key
    //   cur_page_id = internalpage->ValueAt(i - 1);
    // }
    buffer_pool_manager_->UnpinPage(parent_page_id, false);
    page = buffer_pool_manager_->FetchPage(cur_page_id);  // 取出下一页
    bplustreepage = reinterpret_cast<BPlusTreePage *>(page->GetData());
  }
  return cur_page_id;
}
/**
 * Return the only value that associated with input key
 * This method is used for point query
 * @return: true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  if (IsEmpty()) {
    return false;
  }
  // 获得叶子节点编号，
  page_id_t leaf_page_id = GetLeafPageId(key);
  // GetLeafPageId() 已经将leaf page 放入buffer
  auto *leafpage = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(leaf_page_id)->GetData());
  int size = leafpage->GetSize();  // 获得array_数组大小
  for (int i = 0; i < size; ++i) {
    if (comparator_(leafpage->KeyAt(i), key) == 0) {
      result->emplace_back(leafpage->ValueAt(i));
      buffer_pool_manager_->UnpinPage(leaf_page_id, false);
      return true;
    }
  }
  buffer_pool_manager_->UnpinPage(leaf_page_id, false);
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  page_id_t leaf_page_id;
  if (IsEmpty()) {
    // page_id_t page_id;
    auto *leafpage = reinterpret_cast<LeafPage *>(buffer_pool_manager_->NewPage(&leaf_page_id)->GetData());
    leafpage->Init(leaf_page_id, INVALID_PAGE_ID, LEAF_PAGE_SIZE);
    // root_page_id_ = page_id;
    root_page_id_ = leaf_page_id;
    UpdateRootPageId(1);
  } else {  // find the leaf Node that should contain "key"
    leaf_page_id = GetLeafPageId(key);
  }
  auto *leafpage = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(leaf_page_id)->GetData());
  int size = leafpage->GetSize();  // 获得array_数组大小
  int max_size = leafpage->GetMaxSize();
  for (int i = 0; i < size; ++i) {
    if (comparator_(leafpage->KeyAt(i), key) == 0) {         // 已经存在
      buffer_pool_manager_->UnpinPage(leaf_page_id, false);  // 返回前unpin
      return false;
    }
  }
  // std::cout << "leaf_page_id is " << leaf_page_id << std::endl;
  // std::cout << "max_size is " << max_size << std::endl;
  if (size < max_size - 1) {
    // std::cout << "key is " << key << std::endl;
    InsertInLeaf(leafpage, key, value);
    buffer_pool_manager_->UnpinPage(leaf_page_id, true);  // 返回前unpin
    return true;
  }
  // size == max_size - 1, split
  // create a new leaf node L'
  page_id_t rightleafpageid;
  // 申请一个新的叶子节点
  auto *rightleafpage = reinterpret_cast<LeafPage *>(buffer_pool_manager_->NewPage(&rightleafpageid)->GetData());
  rightleafpage->Init(rightleafpageid);
  // 临时节点
  auto *temp = new LeafPage;
  temp->Init(INVALID_PAGE_ID);
  auto src = leafpage->GetPointer(0);
  auto dst = temp->GetPointer(0);
  // 将leafpage中元素都复制到 temp中
  memcpy(static_cast<void *>(dst), static_cast<void *>(src), size * leafpage->GetMappingTypeSize());
  // for(int i = 0; i < size; ++i) {
  //   temp->SetKeyAt(i, leafpage->KeyAt(i));
  //   temp->SetValueAt(i, leafpage->ValueAt(i));
  // }
  temp->SetSize(size);
  // 将 (key, value) 插入到临时节点
  InsertInLeaf(temp, key, value);
  // 调整两个叶子结点的 nextpageid
  rightleafpage->SetNextPageId(leafpage->GetNextPageId());
  leafpage->SetNextPageId(rightleafpageid);
  // 分割临时节点，左右两个叶子节点的(k,v) 数对
  int left_size = (LEAF_PAGE_SIZE + 1) / 2;
  int right_size = LEAF_PAGE_SIZE - left_size;
  // 复制的起点
  auto src1 = temp->GetPointer(0);
  auto src2 = temp->GetPointer(left_size);
  auto leftdst = leafpage->GetPointer(0);
  auto rightdst = rightleafpage->GetPointer(0);  // ???????????? leafpage->rightleafpage
  // 进行复制
  std::memcpy(static_cast<void *>(leftdst), static_cast<void *>(src1), left_size * temp->GetMappingTypeSize());
  std::memcpy(static_cast<void *>(rightdst), static_cast<void *>(src2), right_size * temp->GetMappingTypeSize());
  // 设置左右两个叶子节点的大小
  leafpage->SetSize(left_size);
  rightleafpage->SetSize(right_size);

  // insert in parent
  delete temp;
  KeyType k = rightleafpage->KeyAt(0);
  InsertInParent(leafpage, rightleafpage, k);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInParent(BPlusTreePage *leftpage, BPlusTreePage *rightpage, const KeyType &key) {
  if (leftpage->IsRootPage()) {  // 左节点是根节点, 之前分裂的节点是根节点
    // create a new page as root
    page_id_t root_id;
    auto *rootpage = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(&root_id)->GetData());
    rootpage->Init(root_id);
    leftpage->SetParentPageId(root_id);  // 左右节点父节点信息改变
    rightpage->SetParentPageId(root_id);
    root_page_id_ = root_id;
    UpdateRootPageId(0);  // 更新根节点编号
    rootpage->SetValueAt(0, leftpage->GetPageId());
    rootpage->SetKeyAt(1, key);
    rootpage->SetValueAt(1, rightpage->GetPageId());
    rootpage->SetSize(2);
    buffer_pool_manager_->UnpinPage(root_id, true);                 // unpin the new root page
    buffer_pool_manager_->UnpinPage(leftpage->GetPageId(), true);   // unpin the child page for the change of parent id
    buffer_pool_manager_->UnpinPage(rightpage->GetPageId(), true);  // unpin the child page for the change of parent id
    return;
  }
  // 内部节点的array_类型是KeyType, page_id_t, 和叶子节点不同
  page_id_t parent_id = leftpage->GetParentPageId();
  auto *parentpage = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_id)->GetData());
  int size = parentpage->GetSize();
  int maxsize = parentpage->GetMaxSize();

  if (size < maxsize) {  // 父节点中由足够空间
    // 找到父节点中指向leftpage的(key, page_id)对,
    int index = 0;
    for (int i = size - 1; i >= 0; --i) {  //
      if (parentpage->ValueAt(i) == leftpage->GetPageId()) {
        index = i + 1;  // 在父节点中找到指向leftpage的位置
        break;          // 在之后插入指向rightpage的位置
      }
    }
    auto src = parentpage->GetPointer(index);
    auto dst = parentpage->GetPointer(index + 1);  // 右移
    memmove(static_cast<void *>(dst), static_cast<void *>(src), (size - index) * parentpage->GetMappingTypeSize());
    parentpage->SetKeyAt(index, key);
    parentpage->SetValueAt(index, rightpage->GetPageId());

    rightpage->SetParentPageId(parent_id);  ///

    buffer_pool_manager_->UnpinPage(parent_id, true);               // unpin parent page
    buffer_pool_manager_->UnpinPage(leftpage->GetPageId(), true);   // unpin the child page for the change of parent id
    buffer_pool_manager_->UnpinPage(rightpage->GetPageId(), true);  // unpin the child page for the change of parent id
    return;
  }

  // size == maxsize
  using internalpair = std::pair<KeyType, page_id_t>;
  std::pair<KeyType, page_id_t> temp[size + 1];
  std::memcpy(static_cast<void *>(temp), static_cast<void *>(parentpage->GetPointer(0)),
              size * parentpage->GetMappingTypeSize());
  int index = 0;
  for (int i = size - 1; i >= 1; --i) {
    if (temp[i].second == leftpage->GetPageId()) {
      index = i + 1;
      break;
    }
  }

  std::memmove(static_cast<void *>(&temp[index + 1]), static_cast<void *>(&temp[index]),
               (size - index) * sizeof(internalpair));

  temp[index].first = key;
  temp[index].second = rightpage->GetPageId();

  page_id_t rightparent_id;
  auto *rightparent = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(&rightparent_id)->GetData());
  rightparent->Init(rightparent_id);
  // parent节点要分裂, 申请新的节点
  auto left_dst = parentpage->GetPointer(0);
  auto right_dst = rightparent->GetPointer(0);
  int left_size = (size + 2) / 2;
  int right_size = size + 1 - left_size;
  std::memcpy(static_cast<void *>(left_dst), static_cast<void *>(temp), left_size * sizeof(internalpair));
  std::memcpy(static_cast<void *>(right_dst), static_cast<void *>(&temp[left_size]), right_size * sizeof(internalpair));
  if (index >= left_size) {                      // 指向rightpage的节点放在了rightparent中
    rightpage->SetParentPageId(rightparent_id);  // 一定成立吗
  } else {
    rightpage->SetParentPageId(parent_id);
  }
  parentpage->SetSize(left_size);
  rightparent->SetSize(right_size);                               // 分裂之后，设置左右两个父节点的size
  buffer_pool_manager_->UnpinPage(leftpage->GetPageId(), true);   // unpin the child page for the change of parent id
  buffer_pool_manager_->UnpinPage(rightpage->GetPageId(), true);  // unpin the child page for the change of parent id

  KeyType k = temp[left_size].first;
  InsertInParent(parentpage, rightparent, k);
}
/****************************************************/
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInLeaf(LeafPage *leafpage, const KeyType &key, const ValueType &value) {
  int size = leafpage->GetSize();
  int index = 0;
  // 把key插入有序位置， 找到最大的 k <= key
  for (int i = size - 1; i >= 0; --i) {  // 倒序查找第一个
    KeyType k = leafpage->KeyAt(i);
    if (comparator_(k, key) <= 0) {  // k <= key
      index = i + 1;
      break;
    }
  }
  // index 表示放入的位置
  auto src = leafpage->GetPointer(index);
  auto dst = leafpage->GetPointer(index + 1);
  memmove(static_cast<void *>(dst), static_cast<void *>(src),
          (size - index) * leafpage->GetMappingTypeSize());  // move right
  leafpage->SetKeyAt(index, key);
  leafpage->SetValueAt(index, value);
  leafpage->SetSize(size + 1);

  /*KeyType k = leafpage->KeyAt(0);
  if (comparator_(k, key) == 1) { // key < k
    auto src = leafpage->GetPointer(0);
    auto dst = leafpage->GetPointer(1);
    memmove(dst, src, size*sizeof(MappingType));
    leafpage->SetKeyAt(0, key);
    leafpage->SetValueAt(0, value);
    return ;
  }
  for(int i = size-1; i >= 0; --i) {
    KeyType k = leafpage->KeyAt(i);
    if (comparator_(k, key) <= 0) { // k <= key
      // highest k that is less than or equal to key
      auto src = leafpage->GetPointer(i+1);
      auto dst = leafpage->GetPointer(i+2);
      memmove(dst, src, (size-i-1)*sizeof(MappingType));
      leafpage->SetKeyAt(i+1, key);
      leafpage->SetValueAt(i+1, value);
      return ;
    }
  }*/
}
/*****************************************************************************
 * REMOVE
 *****************************************************************************/

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()) {
    return;
  }
  page_id_t leaf_id = GetLeafPageId(key);
  auto *page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(leaf_id)->GetData());
  DeleteEntry(page, key);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DeleteEntry(BPlusTreePage *page, const KeyType &key) {
  int size = page->GetSize();
  if (page->IsLeafPage()) {
    auto *leafpage = reinterpret_cast<LeafPage *>(page);
    for (int i = 0; i < size; ++i) {
      if (comparator_(leafpage->KeyAt(i), key) == 0) {  // i 就是要被删除的位置
        memmove(static_cast<void *>(leafpage->GetPointer(i)), static_cast<void *>(leafpage->GetPointer(i + 1)),
                (size - i - 1) * leafpage->GetMappingTypeSize());
        leafpage->SetSize(size - 1);
      }
    }
  } else {
    auto *internalpage = reinterpret_cast<InternalPage *>(page);
    for (int i = 1; i < size; ++i) {
      if (comparator_(internalpage->KeyAt(i), key) == 0) {
        memmove(static_cast<void *>(internalpage->GetPointer(i)), static_cast<void *>(internalpage->GetPointer(i + 1)),
                (size - i - 1) * internalpage->GetMappingTypeSize());
        internalpage->SetSize(size - 1);
      }
    }
  }
  // 如何判断是 rootpage() 如果只有一个节点 ？？？？？？

  size = page->GetSize();
  int max_size = page->GetMaxSize();
  if (page->IsLeafPage()) {
    max_size--;
  }
  int min_size = page->GetMinSize();
  // 问题是如果b+树中只有一个叶子节点，这个叶子节点也是根节点，size < 2，显然不能删除节点
  if (page->IsRootPage() && size < min_size) {  // only one pointer left;
    if (page->IsLeafPage()) {                   // 也是叶子节点
      if (size == 0) {                          // 唯一的节点中没有元素了
        buffer_pool_manager_->UnpinPage(root_page_id_, true);
        buffer_pool_manager_->DeletePage(root_page_id_);  // 删除这一页, 清空B+树
        root_page_id_ = INVALID_PAGE_ID;
      }
    } else {  // internalpage
      auto *rootpage = reinterpret_cast<InternalPage *>(page);
      page_id_t child_id = rootpage->ValueAt(0);
      root_page_id_ = child_id;
      UpdateRootPageId(0);  // 更新根节点编号
      auto *child = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(child_id)->GetData());
      child->SetParentPageId(INVALID_PAGE_ID);  // 更新孩子节点的父亲节点信息
      page_id_t root_id = rootpage->GetPageId();
      buffer_pool_manager_->UnpinPage(child_id, true);
      buffer_pool_manager_->UnpinPage(root_id, true);
      buffer_pool_manager_->DeletePage(root_id);  // 删除这一页
    }
    return;
  }
  // 不是 root 或者 page->GetSize() >= page->GetMinSize()
  // ************************** merge or redistribute ********************************
  if (size < min_size) {
    // find siblings
    // 这里的原则是优先找左边的 sibling,
    page_id_t parent_id = page->GetParentPageId();
    page_id_t page_id = page->GetPageId();
    page_id_t sibling_id = INVALID_PAGE_ID;
    KeyType k;         // parent 中指向sibling 或者page 的k
    int k_index = 0;   // 记住k在parent中的下标
    bool flag = true;  // flag 为真，表示sibling_child 在左边
    auto *parent = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_id)->GetData());
    for (int i = 0; i < parent->GetSize(); ++i) {
      if (parent->ValueAt(i) == page_id) {
        if (i == 0) {  // page 是parent的最左边孩子
          sibling_id = parent->ValueAt(1);
          k = parent->KeyAt(1);
          k_index = 1;
          flag = false;
        } else {
          sibling_id = parent->ValueAt(i - 1);
          k = parent->KeyAt(i);
          k_index = i;
        }
        break;
      }
    }
    auto *sibling_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(sibling_id)->GetData());

    int sibling_size = sibling_page->GetSize();

    if (sibling_size + size <= max_size) {  // ************** 合并 ***************
      if (!flag) {                          // flag 为假， page is predecessor of sibling page
        BPlusTreePage *temp = sibling_page;
        sibling_page = page;
        page = temp;
      }
      sibling_id = sibling_page->GetPageId();
      page_id = page->GetPageId();

      sibling_size = sibling_page->GetSize();
      size = page->GetSize();
      // 现在sibling page 在page 左边
      if (page->IsLeafPage()) {  // **************** 叶子节点合并 ****************
        auto *leaf_sibling_page = reinterpret_cast<LeafPage *>(sibling_page);
        auto *leaf_page = reinterpret_cast<LeafPage *>(page);
        // 将leaf_page 中 所有的k/v复制到 leaf_sibling_page中
        auto dst = leaf_sibling_page->GetPointer(sibling_size);
        auto src = leaf_page->GetPointer(0);
        memcpy(static_cast<void *>(dst), static_cast<void *>(src), size * leaf_page->GetMappingTypeSize());
        // 设置合并之后的节点size
        leaf_sibling_page->SetSize(sibling_size + size);
        leaf_sibling_page->SetNextPageId(leaf_page->GetNextPageId());  // 设置 next_page_id
      } else {  // ******************** 非叶子节点合并 *****************
        auto *internal_sibling_page = reinterpret_cast<InternalPage *>(sibling_page);
        auto *internal_page = reinterpret_cast<InternalPage *>(page);
        internal_page->SetKeyAt(0, k);
        auto dst = internal_sibling_page->GetPointer(sibling_size);
        auto src = internal_page->GetPointer(0);
        memcpy(static_cast<void *>(dst), static_cast<void *>(src), size * internal_page->GetMappingTypeSize());
        internal_sibling_page->SetSize(sibling_size + size);
      }
      buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);  // unpin 左边孩子
      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
      buffer_pool_manager_->DeletePage(page->GetPageId());  // unpin 右边孩子并且删除节点
      // 继续在父节点删除指向右边孩子的k
      DeleteEntry(parent, k);
    } else {                       //****************************** 重分配 ******************
      if (flag) {                  // flag为真，sibling_page 在左边 （sibling_page, page)
        if (page->IsLeafPage()) {  // ***********叶子节点重分配***************
          auto *leaf_sibling_page = reinterpret_cast<LeafPage *>(sibling_page);
          auto *leaf_page = reinterpret_cast<LeafPage *>(page);
          // 删除左孩子中最后一个（last_k,last_v)
          KeyType last_k = leaf_sibling_page->KeyAt(sibling_size - 1);
          ValueType last_v = leaf_sibling_page->ValueAt(sibling_size - 1);
          leaf_sibling_page->SetSize(sibling_size - 1);  // 没有移动数据只是 修改大小  ????????
          // 右孩子中(k,v) 右移
          auto src = leaf_page->GetPointer(0);
          auto dst = leaf_page->GetPointer(1);
          memmove(static_cast<void *>(dst), static_cast<void *>(src), size * leaf_page->GetMappingTypeSize());
          // 将(last_k, last_v) 设为右孩子第一个节点
          leaf_page->SetKeyAt(0, last_k);
          leaf_page->SetValueAt(0, last_v);
          leaf_page->SetSize(size + 1);
          // 将parent中指向右边孩子的k 设置为last_k, 保持有序性
          parent->SetKeyAt(k_index, last_k);
          // unpin 左右孩子，已经父节点
          buffer_pool_manager_->UnpinPage(parent_id, true);
          buffer_pool_manager_->UnpinPage(sibling_id, true);
          buffer_pool_manager_->UnpinPage(page_id, true);
        } else {  // ******************* 非叶子结点重分配**************
          auto *internal_sibling_page = reinterpret_cast<InternalPage *>(sibling_page);  // 左边
          auto *internal_page = reinterpret_cast<InternalPage *>(page);
          // 删除左边孩子的最后一个 (last_k, last_v)
          KeyType last_k = internal_sibling_page->KeyAt(sibling_size - 1);
          page_id_t last_v = internal_sibling_page->ValueAt(sibling_size - 1);
          internal_sibling_page->SetSize(sibling_size - 1);
          // 将右边第一个key设为父节点中指向右边孩子的k, 将所有k/v右移
          internal_page->SetKeyAt(0, k);
          auto src = internal_page->GetPointer(0);
          auto dst = internal_page->GetPointer(1);
          memmove(static_cast<void *>(dst), static_cast<void *>(src), size * internal_page->GetMappingTypeSize());
          // 右移之后将第一个value设为做孩子最后一个value
          internal_page->SetValueAt(0, last_v);
          internal_page->SetSize(size + 1);
          // 右孩子第一个k变了，修改parent中指向右孩子的k为左孩子的最后一个key
          parent->SetKeyAt(k_index, last_k);
          // unpin 左右两个孩子以及父亲节点
          buffer_pool_manager_->UnpinPage(sibling_id, true);
          buffer_pool_manager_->UnpinPage(page_id, true);
          buffer_pool_manager_->UnpinPage(parent_id, true);
        }
      } else {                             // flag 为假， page 在左边 sibling_page 在右边 (page, sibling_page)
        if (sibling_page->IsLeafPage()) {  // ***********叶子节点重分配***************
          auto *leaf_page = reinterpret_cast<LeafPage *>(page);                  // 左边
          auto *leaf_sibling_page = reinterpret_cast<LeafPage *>(sibling_page);  // 右边
          // 删除左孩子中最后一个last_k,last_v
          KeyType last_k = leaf_page->KeyAt(size - 1);
          ValueType last_v = leaf_page->ValueAt(size - 1);
          leaf_page->SetSize(size - 1);
          // 右孩子中(k,v)右移
          auto src = leaf_sibling_page->GetPointer(0);
          auto dst = leaf_sibling_page->GetPointer(1);
          memmove(static_cast<void *>(dst), static_cast<void *>(src),
                  sibling_size * leaf_sibling_page->GetMappingTypeSize());
          // 将(last_k, last_v) 设为右孩子第一个节点
          leaf_sibling_page->SetKeyAt(0, last_k);
          leaf_sibling_page->SetValueAt(0, last_v);
          leaf_sibling_page->SetSize(sibling_size + 1);
          // 将parent中指向右边孩子的k 设置为last_k, 保持有序性
          parent->SetKeyAt(k_index, last_k);
          // unpin 左右孩子，以及父节点
          buffer_pool_manager_->UnpinPage(parent_id, true);
          buffer_pool_manager_->UnpinPage(sibling_id, true);
          buffer_pool_manager_->UnpinPage(page_id, true);
        } else {  // ******************* 非叶子结点重分配**************
          auto *internal_page = reinterpret_cast<InternalPage *>(page);  // 左边
          auto *internal_sibling_page = reinterpret_cast<InternalPage *>(sibling_page);
          // 删除左边孩子的最后一个 (last_k, last_v)
          KeyType last_k = internal_page->KeyAt(size - 1);
          page_id_t last_v = internal_page->ValueAt(size - 1);
          internal_page->SetSize(size - 1);
          // 将右边第一个key设为父节点中指向右边孩子的k, 将所有k/v右移
          internal_sibling_page->SetKeyAt(0, k);
          auto src = internal_sibling_page->GetPointer(0);
          auto dst = internal_sibling_page->GetPointer(1);
          memmove(static_cast<void *>(dst), static_cast<void *>(src),
                  sibling_size * internal_sibling_page->GetMappingTypeSize());
          // 右移之后将第一个value设为左孩子最后一个value
          internal_sibling_page->SetValueAt(0, last_v);
          internal_sibling_page->SetSize(sibling_size + 1);
          // 右孩子第一个k变了，修改parent中指向右孩子的k为左孩子的最后一个key
          parent->SetKeyAt(k_index, last_k);
          // unpin 左右两个孩子以及父亲节点
          buffer_pool_manager_->UnpinPage(sibling_id, true);
          buffer_pool_manager_->UnpinPage(page_id, true);
          buffer_pool_manager_->UnpinPage(parent_id, true);
        }
      }
    }
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
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

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
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
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
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
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