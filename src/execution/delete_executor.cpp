//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"
#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  table_id_ = plan_->TableOid();  // 要删除的表
  table_info_ = exec_ctx_->GetCatalog()->GetTable(table_id_);
  heap_ = table_info_->table_.get();
  std::string table_name = table_info_->name_;
  index_info_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_name);  // 这张表所有的index
  child_executor_->Init();
  txn_ = exec_ctx_->GetTransaction();
  try {
    exec_ctx_->GetLockManager()->LockTable(txn_, LockManager::LockMode::INTENTION_EXCLUSIVE, table_id_);
  } catch (bustub::TransactionAbortException &ex) {
#ifndef NDEBUG
    LOG_ERROR("Error Encountered in Executor Execution: %s", ex.what());
#endif
    throw bustub::ExecutionException("Delete executor LockTable Fail");
  }
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  std::vector<RID> ridvec;
  std::vector<Tuple> tuplevec;
  Tuple t;
  RID r;
  while (child_executor_->Next(&t, &r)) {
    ridvec.emplace_back(r);
    tuplevec.emplace_back(t);
  }
  if (ridvec.empty() && has_output_) {
    return false;
  }

  int64_t n = ridvec.size();
  Value v(TypeId::BIGINT, n);
  for (int64_t i = 0; i < n; ++i) {
    try {  // 加锁
      exec_ctx_->GetLockManager()->LockRow(txn_, LockManager::LockMode::EXCLUSIVE, table_id_, ridvec[i]);
    } catch (bustub::TransactionAbortException &ex) {
#ifndef NDEBUG
      LOG_ERROR("Error Encountered in Executor Execution: %s", ex.what());
#endif
      throw bustub::ExecutionException("Delete executor LockRow Fail");
      return false;
    }
    heap_->MarkDelete(ridvec[i], exec_ctx_->GetTransaction());
    for (const auto &index : index_info_) {
      const auto &key_attrs = index->index_->GetMetadata()->GetKeyAttrs();
      const auto &schema = table_info_->schema_;
      const auto &key_schema = index->key_schema_;
      index->index_->DeleteEntry(tuplevec[i].KeyFromTuple(schema, key_schema, key_attrs), ridvec[i],
                                 exec_ctx_->GetTransaction());
    }
  }

  char storage[BUSTUB_PAGE_SIZE];
  *reinterpret_cast<uint32_t *>(storage) = sizeof(v);
  v.SerializeTo(storage + sizeof(uint32_t));
  t.DeserializeFrom(storage);
  *tuple = t;
  has_output_ = true;
  return true;
}

}  // namespace bustub
