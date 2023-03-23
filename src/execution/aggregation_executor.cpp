//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.End()),
      schema_(AggregationPlanNode::InferAggSchema(plan_->GetGroupBys(), plan_->GetAggregates(),
                                                  plan_->GetAggregateTypes())) {}

void AggregationExecutor::Init() {
  child_->Init();
  Tuple tuple;
  RID rid;
  while (child_->Next(&tuple, &rid)) {  // 考虑空表
    aht_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aht_iterator_ == aht_.End() && has_output_) {
    return false;
  }
  std::vector<Value> values;
  if (aht_iterator_ == aht_.End()) {  // 空表
    if (!plan_->GetGroupBys().empty()) {
      return false;
    }
    for (const auto &a : plan_->GetGroupBys()) {
      values.emplace_back(ValueFactory::GetNullValueByType(a->GetReturnType()));
    }
    for (const auto &a : aht_.GenerateInitialAggregateValue().aggregates_) {
      values.emplace_back(a);
    }
  } else {
    for (const auto &a : aht_iterator_.Key().group_bys_) {
      values.emplace_back(a);
    }
    for (const auto &a : aht_iterator_.Val().aggregates_) {
      values.emplace_back(a);
    }
  }
  Tuple t(values, &(plan_->OutputSchema()));
  *tuple = t;
  if (aht_iterator_ != aht_.End()) {
    ++aht_iterator_;
  }
  has_output_ = true;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
