//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  table_oid_t table_id = plan_->TableOid();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(table_id);
  heap_ = table_info_->table_.get();
  std::string table_name = table_info_->name_;
  index_info_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_name);
  child_executor_->Init();
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  std::vector<Tuple> vec;
  Tuple t;
  RID r;
  while (child_executor_->Next(&t, &r)) {
    vec.push_back(t);
  }
  if (vec.empty() && has_output_) { // 孩子节点没有向上返回要删除的tuple，并且之前已经有了输出
    return false;
  }
  int64_t n = vec.size();
  Value v(TypeId::BIGINT,n);
  for(int64_t i = 0; i < n; ++i) {
    heap_->InsertTuple(vec[i], &r, exec_ctx_->GetTransaction());
    for (const auto &index : index_info_) {
      const auto& key_attrs = index->index_->GetMetadata()->GetKeyAttrs();
      const auto& schema = table_info_->schema_;
      const auto& key_schema = index->key_schema_;
      index->index_->InsertEntry(vec[i].KeyFromTuple(schema, key_schema, key_attrs), r, exec_ctx_->GetTransaction());
    }
  }
  char storage[BUSTUB_PAGE_SIZE];
  *reinterpret_cast<uint32_t*>(storage) = sizeof(v);
  v.SerializeTo(storage+sizeof(uint32_t));
  t.DeserializeFrom(storage);
  *tuple = t;
  has_output_ = true;
  return true;
}

}  // namespace bustub
