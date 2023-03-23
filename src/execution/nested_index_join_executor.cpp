//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "common/logger.h"
#include "concurrency/transaction.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      left_schema_(child_executor_->GetOutputSchema()),
      right_schema_(plan_->InnerTableSchema()) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() {
  child_executor_->Init();

  left_column_count_ = left_schema_.GetColumnCount();
  right_column_count_ = right_schema_.GetColumnCount();

  table_oid_t inner_table_id = plan_->GetInnerTableOid();
  right_table_info_ = exec_ctx_->GetCatalog()->GetTable(inner_table_id);
  right_heap_ = right_table_info_->table_.get();

  index_oid_t index_id = plan_->GetIndexOid();
  right_index_info_ = exec_ctx_->GetCatalog()->GetIndex(index_id);
  tree_ = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(right_index_info_->index_.get());

  if (plan_->GetJoinType() == JoinType::LEFT) {
    for (size_t i = 0; i < right_column_count_; ++i) {
      TypeId type = right_schema_.GetColumn(i).GetType();
      null_values_.push_back(ValueFactory::GetNullValueByType(type));
    }
  }
}
// no duplicate rows into tables with indexes.
auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple left_tuple;
  RID r;
  while (child_executor_->Next(&left_tuple, &r)) {  // 获得左边的tuple
    Tuple right_tuple;
    Value val = plan_->KeyPredicate()->Evaluate(&left_tuple, left_schema_);
    if (val.IsNull()) {
      return false;
    }
    Tuple key_tuple({val}, &(right_index_info_->key_schema_));
    std::vector<RID> result;
    bool res = tree_->ScanKey(key_tuple, &result, exec_ctx_->GetTransaction());

    std::vector<Value> values;

    for (size_t i = 0; i < left_column_count_; ++i) {
      values.push_back(left_tuple.GetValue(&left_schema_, i));
    }
    if (res) {  // 存在
      r = result[0];
      right_heap_->GetTuple(r, &right_tuple, exec_ctx_->GetTransaction());
      for (size_t i = 0; i < right_column_count_; ++i) {
        values.push_back(right_tuple.GetValue(&right_schema_, i));
      }
      *tuple = Tuple(values, &plan_->OutputSchema());
      return true;
    }
    if (plan_->GetJoinType() == JoinType::LEFT) {
      for (const auto &val : null_values_) {
        values.push_back(val);
      }
      *tuple = Tuple(values, &plan_->OutputSchema());
      return true;
    }
  }
  return false;
}

}  // namespace bustub
