#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
  // std::cout << GREEN << "OptimizeSortLimitAsTopN" << END << std::endl;  
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::Limit) { // 当前节点是limit plan
    const auto &limit_plan = dynamic_cast<const LimitPlanNode &>(*optimized_plan);
    BUSTUB_ENSURE(optimized_plan->children_.size() == 1, "Limit with multiple children?? That's weird!");

    const auto &child_plan = optimized_plan->children_[0];

    if (child_plan->GetType() == PlanType::Sort) {  // 并且孩子节点是 sort plan
      const auto &sort_plan = dynamic_cast<const SortPlanNode &>(*child_plan);
      BUSTUB_ENSURE(child_plan->children_.size() == 1, "Sort should have only one child");
      return std::make_shared<TopNPlanNode>(child_plan->output_schema_, child_plan->children_[0],sort_plan.order_bys_, limit_plan.limit_); 
    }
  }

  return optimized_plan;  // 原本没有优化，

}

}  // namespace bustub
