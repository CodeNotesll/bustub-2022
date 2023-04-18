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
#include "common/exception.h"
#include "common/rid.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"
#include "execution/executor_context.h"
#include "storage/table/table_iterator.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), table_iterator_(nullptr, RID{}, nullptr) {}

void SeqScanExecutor::Init() {
  table_id_ = plan_->GetTableOid();
  TableInfo *table_info = exec_ctx_->GetCatalog()->GetTable(table_id_);
  heap_ = table_info->table_.get();
  txn_ = exec_ctx_->GetTransaction();
  table_iterator_ = heap_->Begin(txn_);
  IsolationLevel iso = txn_->GetIsolationLevel();
  // 读-未提交 不加读锁
  if (iso != IsolationLevel::READ_UNCOMMITTED) {
    try {
      exec_ctx_->GetLockManager()->LockTable(txn_, LockManager::LockMode::INTENTION_SHARED, table_id_);
    } catch (bustub::TransactionAbortException &ex) {
#ifndef NDEBUG
      LOG_ERROR("Error Encountered in Executor Execution: %s", ex.what());
#endif
      throw bustub::ExecutionException("Seq_scan_executor LockTable Fail");
    }
  }
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  IsolationLevel iso = txn_->GetIsolationLevel();
  if (table_iterator_ == heap_->End()) {
    if (iso == IsolationLevel::READ_COMMITTED) {  // 读可提交隔离机制 提前释放锁
      for (const auto &rid : rids_) {             // 释放row锁
        try {
          exec_ctx_->GetLockManager()->UnlockRow(txn_, table_id_, rid);
        } catch (bustub::TransactionAbortException &ex) {
#ifndef NDEBUG
          LOG_ERROR("Error Encountered in Executor Execution: %s", ex.what());
#endif
          throw bustub::ExecutionException("Seq_scan_executor UnlockRow Fail");
        }
      }
      try {  // 释放table 锁
        exec_ctx_->GetLockManager()->UnlockTable(txn_, table_id_);
      } catch (bustub::TransactionAbortException &ex) {
#ifndef NDEBUG
        LOG_ERROR("Error Encountered in Executor Execution: %s", ex.what());
#endif
        throw bustub::ExecutionException("Seq_scan_executor UnlockTable Fail");
      }
    }
    return false;
  }
  *tuple = *(table_iterator_);  // 赋值运算符
  *rid = tuple->GetRid();
  rids_.push_back(*rid);
  if (iso != IsolationLevel::READ_UNCOMMITTED) {
    try {
      exec_ctx_->GetLockManager()->LockRow(txn_, LockManager::LockMode::SHARED, table_id_, tuple->GetRid());
    } catch (bustub::TransactionAbortException &ex) {
#ifndef NDEBUG
      LOG_ERROR("Error Encountered in Executor Execution: %s", ex.what());
#endif
      throw bustub::ExecutionException("Seq_scan_executor LockRow Fail");
      return false;
    }
  }
  // if (txn_->GetState() == TransactionState::ABORTED) {
  //   return false;
  // }
  ++table_iterator_;
  return true;
}

}  // namespace bustub
