#include "common/logger.h"
#include "execution/plans/abstract_plan.h"
#include "optimizer/optimizer.h"

// Note for 2022 Fall: You can add all optimizer rule implementations and apply the rules as you want in this file. Note
// that for some test cases, we force using starter rules, so that the configuration here won't take effects. Starter
// rule can be forcibly enabled by `set force_optimizer_starter_rule=yes`.

namespace bustub {

auto Optimizer::OptimizeCustom(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  //std::cout << "OPtimizeCustom called" << std::endl;
  auto p = plan;
  p = OptimizeMergeProjection(p);
  //std::cout << "After MergerProjection : \n";
  // std::cout << GREEN << p->ToString() << END << "\n";
  p = OptimizeMergeFilterNLJ(p);
  //std::cout << "After MergerFilter : \n";
  //  std::cout << GREEN << p->ToString() << END << "\n";
  p = OptimizeNLJAsIndexJoin(p);
  //std::cout << "After IndexJoin : \n";
  //  std::cout << GREEN << p->ToString() << END << "\n";
  // p = OptimizeNLJAsHashJoin(p);  // Enable this rule after you have implemented hash join.
  p = OptimizeOrderByAsIndexScan(p);
  //std::cout << "After IndexScan : \n";
  // std::cout << GREEN << p->ToString() << END << "\n";
  p = OptimizeSortLimitAsTopN(p);
  //std::cout << "After TopN : \n";
  //  std::cout << GREEN << p->ToString() << END << "\n";
  return p;
}

}  // namespace bustub
