//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan){}    // 没有孩子，作为leaf节点

void IndexScanExecutor::Init() { 
    index_id_ = plan_->GetIndexOid();
    index_info_ = exec_ctx_->GetCatalog()->GetIndex(index_id_);
    std::string table_name = index_info_->table_name_;
    TableInfo* table_info = exec_ctx_->GetCatalog()->GetTable(table_name);
    heap_ = table_info->table_.get();
    tree_ = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info_->index_.get());
    tree_iterator_ = tree_->GetBeginIterator();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    if (tree_iterator_.IsEnd()) {
        return false;
    }
    *rid = (*tree_iterator_).second;
    heap_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction());
    ++ tree_iterator_;
    return true;
}

}  // namespace bustub
