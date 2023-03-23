//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)),
      left_schema_(plan_->GetLeftPlan()->OutputSchema()),
      right_schema_(plan_->GetRightPlan()->OutputSchema()) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  Tuple t;
  RID r;
  while (left_executor_->Next(&t, &r)) {
    left_tuples_.emplace_back(t);
  }
  while (right_executor_->Next(&t, &r)) {
    right_tuples_.emplace_back(t);
  }
  left_col_size_ = left_schema_.GetColumns().size();
  right_col_size_ = right_schema_.GetColumns().size();

  for (size_t i = 0; i < right_col_size_; ++i) {  // 根据right_tuple_每个column类型 计算null_value_
    TypeId type = right_schema_.GetColumn(i).GetType();
    null_values_.emplace_back(ValueFactory::GetNullValueByType(type));
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  std::vector<Value> values;
  while (left_idx_ < left_tuples_.size()) {
    while (right_idx_ != right_tuples_.size()) {
      Value val = plan_->Predicate().EvaluateJoin(&(left_tuples_[left_idx_]), left_schema_,
                                                  &(right_tuples_[right_idx_]), right_schema_);
      right_idx_++;                              // 下一个right_tuple_
      if (!val.IsNull() && val.GetAs<bool>()) {  // match
        // concaten
        for (size_t i = 0; i < left_col_size_; ++i) {  // 左边tuple
          values.emplace_back(left_tuples_[left_idx_].GetValue(&left_schema_, i));
        }

        for (size_t i = 0; i < right_col_size_; ++i) {  // 右边tuple
          values.emplace_back(right_tuples_[right_idx_].GetValue(&right_schema_, i));
        }
        *tuple = Tuple(values, &plan_->OutputSchema());
        return true;
      }
    }
    right_idx_ = 0;                                  // 下一个left_tuple_从头开始查找
    if (plan_->GetJoinType() == JoinType::LEFT) {    // left join没有找到match
      for (size_t i = 0; i < left_col_size_; ++i) {  // 左边tuple
        values.emplace_back(left_tuples_[left_idx_].GetValue(&left_schema_, i));
      }

      for (const auto &val : null_values_) {
        values.emplace_back(val);
      }

      left_idx_++;
      *tuple = Tuple(values, &plan_->OutputSchema());
      return true;
    }
    left_idx_++;
  }

  return false;
}

}  // namespace bustub
