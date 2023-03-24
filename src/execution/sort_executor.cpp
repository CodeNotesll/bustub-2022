#include "execution/executors/sort_executor.h"
#include "common/exception.h"
#include "common/rid.h"
#include "execution/executors/values_executor.h"
#include "storage/table/tuple.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    :AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)){}

void SortExecutor::Init() { 
    child_executor_->Init();
    Tuple tuple;
    RID r;
    while (child_executor_->Next(&tuple, &r)) {
        to_sort_tuples_.emplace_back(tuple);
    } 
    Value left_value;
    Value right_value;
    auto cmp = [&] (const Tuple& left, const Tuple& right) {  // 返回true, left 排在right前
        for(const auto& [type, expr] : plan_->GetOrderBy()) {
            if (type == OrderByType::INVALID) {
                throw bustub::Exception("Invalid OrderByType");
            } 
            left_value = expr->Evaluate(&left, child_executor_->GetOutputSchema());
            right_value = expr->Evaluate(&right, child_executor_->GetOutputSchema());
            if (left_value.CompareEquals(right_value) == CmpBool::CmpTrue) {
                continue;
            }
            auto res = left_value.CompareLessThan(right_value); 
            if (type == OrderByType::DESC ) {
                return res == CmpBool::CmpFalse; // 如果res == CmpFalse, 则 left > right, 返回true, 递减排列
            }  // 递减
            // 递增
            return res == CmpBool::CmpTrue;
        }
        return false;
    };
    std::sort(to_sort_tuples_.begin(), to_sort_tuples_.end(), cmp);
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    if (idx_ == to_sort_tuples_.size()) {
        return false;
    }
    *tuple = to_sort_tuples_[idx_];
    idx_++;
    return true;
}

}  // namespace bustub
