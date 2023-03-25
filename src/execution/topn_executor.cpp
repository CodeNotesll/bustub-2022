#include "execution/executors/topn_executor.h"
#include "common/rid.h"
#include "storage/table/tuple.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)){}

void TopNExecutor::Init() { 
    child_executor_->Init();
    Value left_value;
    Value right_value;
    auto cmp = [&](const Tuple& left, const Tuple &right) {
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
    std::priority_queue<Tuple, std::vector<Tuple>, decltype(cmp)> pque(cmp);
    Tuple t;
    RID r;
    while (child_executor_->Next(&t, &r)) {
        pque.push(t);
        if (pque.size() == plan_->n_ + 1) {
            pque.pop();
        }
    }
    while (!pque.empty()) {
        vec_.emplace_back(pque.top());
        pque.pop();
    }
    std::reverse(vec_.begin(), vec_.end());
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    if (idx_ == vec_.size()) {
        return false;
    }
    *tuple = vec_[idx_];
    idx_++;
    return true;
}

}  // namespace bustub
