//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "common/rid.h"
#include "storage/table/table_iterator.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), table_iterator_(nullptr, RID{}, nullptr) {}

void SeqScanExecutor::Init() {
  table_oid_t table_id = plan_->GetTableOid();
  TableInfo *table_info = exec_ctx_->GetCatalog()->GetTable(table_id);
  heap_ = table_info->table_.get();
  Transaction *txn = exec_ctx_->GetTransaction();
  table_iterator_ = heap_->Begin(txn);
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (table_iterator_ == heap_->End()) {
    return false;
  }
  *tuple = *(table_iterator_++);  // 赋值运算符
  *rid = tuple->GetRid();
  return true;
}

}  // namespace bustub
