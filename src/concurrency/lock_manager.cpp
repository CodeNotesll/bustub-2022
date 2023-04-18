//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include <algorithm>
#include <cassert>
#include <functional>
#include <iterator>
#include <map>
#include <memory>
#include <stdexcept>
#include <unordered_map>
#include <vector>

#include "common/config.h"
#include "common/logger.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#define GREEN "\033[32m"
#define RED "\033[31m"
#define YELLOW "\033[33m"
#define END "\033[0m"
#define WHITE 0  // unvisted
#define GRAY 1   // visiting
#define BLACK 2  // visited，节点以及相邻节点被访问过
namespace bustub {

void LockManager::UnLockRowInfo(Transaction *txn, const table_oid_t &oid, const RID &rid) {
  txn_id_t txn_id = txn->GetTransactionId();
  std::cout << "txn: " << txn_id << " unlock on table " << oid << ", row " << rid << std::endl;
}

void LockManager::UnLockTableInfo(Transaction *txn, const table_oid_t &oid) {
  txn_id_t txn_id = txn->GetTransactionId();
  std::cout << "txn: " << txn_id << " unlock on table " << oid << std::endl;
}

void LockManager::LockTableInfo(Transaction *txn, const table_oid_t &oid, LockMode lock_mode) {
  txn_id_t txn_id = txn->GetTransactionId();
  switch (lock_mode) {
    case LockMode::SHARED:
      std::cout << "txn: " << txn_id << " request lock on table " << oid << " in S mode" << std::endl;
      break;
    case LockMode::EXCLUSIVE:
      std::cout << "txn: " << txn_id << " request lock on table " << oid << " in X mode" << std::endl;
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      std::cout << "txn: " << txn_id << " request lock on table " << oid << " in IX mode" << std::endl;
      break;
    case LockMode::INTENTION_SHARED:
      std::cout << "txn: " << txn_id << " request lock on table " << oid << " in IS mode" << std::endl;
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      std::cout << "txn: " << txn_id << " request lock on table " << oid << " in SIX mode" << std::endl;
      break;
  }
}
void LockManager::LockRowInfo(Transaction *txn, const table_oid_t &oid, const RID &rid, LockMode lock_mode) {
  txn_id_t txn_id = txn->GetTransactionId();
  switch (lock_mode) {
    case LockMode::EXCLUSIVE:
      std::cout << txn_id << " X lock on table " << oid << " row: " << rid << std::endl;
      break;
    case LockMode::SHARED:
      std::cout << txn_id << " S lock on table " << oid << " row " << rid << std::endl;
      break;
    default:
      break;
  }
}

void LockManager::UpdateTableLockSet(Transaction *txn, table_oid_t oid, LockMode lock_mode, bool add) {
  // txn_id_t txn_id = txn->GetTransactionId();
  switch (lock_mode) {
    case LockMode::SHARED: {
      auto s_lock_set = txn->GetSharedTableLockSet();
      if (add) {
        s_lock_set->insert(oid);
      } else {
        assert(s_lock_set->find(oid) != s_lock_set->end());
        s_lock_set->erase(oid);
      }
      break;
    }
    case LockMode::SHARED_INTENTION_EXCLUSIVE: {
      auto six_lock_set = txn->GetSharedIntentionExclusiveTableLockSet();
      if (add) {
        six_lock_set->insert(oid);
      } else {
        assert(six_lock_set->find(oid) != six_lock_set->end());
        six_lock_set->erase(oid);
      }
      break;
    }
    case LockMode::EXCLUSIVE: {
      auto x_lock_set = txn->GetExclusiveTableLockSet();
      if (add) {
        x_lock_set->insert(oid);
      } else {
        assert(x_lock_set->find(oid) != x_lock_set->end());
        x_lock_set->erase(oid);
      }
      break;
    }
    case LockMode::INTENTION_EXCLUSIVE: {
      auto ix_lock_set = txn->GetIntentionExclusiveTableLockSet();
      if (add) {
        ix_lock_set->insert(oid);
      } else {
        assert(ix_lock_set->find(oid) != ix_lock_set->end());
        ix_lock_set->erase(oid);
      }
      break;
    }
    case LockMode::INTENTION_SHARED: {
      auto is_lock_set = txn->GetIntentionSharedTableLockSet();
      if (add) {
        is_lock_set->insert(oid);
      } else {
        assert(is_lock_set->find(oid) != is_lock_set->end());
        is_lock_set->erase(oid);
      }
      break;
    }
  }
}

void LockManager::UpdateRowLockSet(Transaction *txn, table_oid_t oid, RID rid, LockMode lockmode, bool add) {
  auto s_row_lock_set = txn->GetSharedRowLockSet();      // map: table_oid_t ---> rid
  auto x_row_lock_set = txn->GetExclusiveRowLockSet();   //
  auto shared_lock_set = txn->GetSharedLockSet();        // set: rid
  auto exclusive_lock_set = txn->GetExclusiveLockSet();  // set: rid
  // txn_id_t txn_id = txn->GetTransactionId();
  switch (lockmode) {
    case LockMode::EXCLUSIVE: {
      if (add) {
        exclusive_lock_set->insert(rid);
        (*x_row_lock_set)[oid].insert(rid);
      } else {
        assert(exclusive_lock_set->find(rid) != exclusive_lock_set->end());
        exclusive_lock_set->erase(rid);

        assert(x_row_lock_set->find(oid) != x_row_lock_set->end());
        assert((*x_row_lock_set)[oid].find(rid) != (*x_row_lock_set)[oid].end());

        (*x_row_lock_set)[oid].erase(rid);
      }
      break;
    }
    case LockMode::SHARED: {
      if (add) {
        shared_lock_set->insert(rid);
        (*s_row_lock_set)[oid].insert(rid);
      } else {
        assert(shared_lock_set->find(rid) != shared_lock_set->end());
        shared_lock_set->erase(rid);
        assert(s_row_lock_set->find(oid) != s_row_lock_set->end());
        assert((*s_row_lock_set)[oid].find(rid) != (*s_row_lock_set)[oid].end());
        (*s_row_lock_set)[oid].erase(rid);
      }
      break;
    }
    default:
      txn->SetState(TransactionState::ABORTED);
      throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }
}
void LockManager::UpdateTxnState(Transaction *txn, LockMode lock_mode) {
  if (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED) {
    return;
  }
  if (txn->GetState() != TransactionState::GROWING) {
    return;
  }
  // lock_mode == s || lock_mode == x
  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::READ_COMMITTED:
      if (lock_mode == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::SHRINKING);
      }
      // growing 状态下 读可提交的隔离度解开s锁不改变2pl
      break;
    case IsolationLevel::READ_UNCOMMITTED:
      if (lock_mode == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::SHRINKING);
      } else {  // s mode under read_uncommited state checkCompatible() read_uncommited状态下不应该有 s mode
        txn->SetState(TransactionState::ABORTED);
        throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
      break;
    case IsolationLevel::REPEATABLE_READ:
      txn->SetState(TransactionState::SHRINKING);
      break;
  }
}
void LockManager::CheckCompatible(Transaction *txn, LockMode lock_mode) {
  if (txn->GetState() == TransactionState::ABORTED || txn->GetState() == TransactionState::COMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    // throw std::logic_error("Lock request in ABORTED, COMMITTED state");
    throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
    switch (txn->GetIsolationLevel()) {
      case IsolationLevel::REPEATABLE_READ:
        txn->SetState(TransactionState::ABORTED);
        throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
        break;
      case IsolationLevel::READ_COMMITTED:
        if (!(lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::SHARED)) {
          txn->SetState(TransactionState::ABORTED);
          throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
        }
        return;
      case IsolationLevel::READ_UNCOMMITTED:
        if (lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::EXCLUSIVE) {
          txn->SetState(TransactionState::ABORTED);
          throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
        } else {
          txn->SetState(TransactionState::ABORTED);
          throw bustub::TransactionAbortException(txn->GetTransactionId(),
                                                  AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
        }
        return;
    }
  }
  // growing state
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {  // S, SIX, IS not allowed
    if (!(lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::EXCLUSIVE)) {
      txn->SetState(TransactionState::ABORTED);
      throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
  }
}

auto LockManager::CheckTableLock(Transaction *txn, table_oid_t oid) -> std::pair<bool, LockMode> {
  if (txn->IsTableExclusiveLocked(oid)) {
    return {true, LockMode::EXCLUSIVE};
  }
  if (txn->IsTableIntentionExclusiveLocked(oid)) {
    return {true, LockMode::INTENTION_EXCLUSIVE};
  }
  if (txn->IsTableSharedIntentionExclusiveLocked(oid)) {
    return {true, LockMode::SHARED_INTENTION_EXCLUSIVE};
  }
  if (txn->IsTableIntentionSharedLocked(oid)) {
    return {true, LockMode::INTENTION_SHARED};
  }
  if (txn->IsTableSharedLocked(oid)) {
    return {true, LockMode::SHARED};
  }
  return {false, LockMode::SHARED};
}
/*
 * IS -> [S, X, IX, SIX]
 * S -> [X, SIX]
 * IX -> [X, SIX]
 * SIX -> [X]
 */
void LockManager::CheckLockUpgrade(Transaction *txn, LockMode held_lock_mode, LockMode lock_mode) {
  txn_id_t txn_id = txn->GetTransactionId();
  switch (held_lock_mode) {
    case LockMode::INTENTION_SHARED:
      return;
    case LockMode::SHARED:
    case LockMode::INTENTION_EXCLUSIVE:
      if (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw bustub::TransactionAbortException(txn_id, AbortReason::INCOMPATIBLE_UPGRADE);
      }
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      if (lock_mode != LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw bustub::TransactionAbortException(txn_id, AbortReason::INCOMPATIBLE_UPGRADE);
      }
      break;
    case LockMode::EXCLUSIVE:
      txn->SetState(TransactionState::ABORTED);
      throw bustub::TransactionAbortException(txn_id, AbortReason::INCOMPATIBLE_UPGRADE);
      break;
  }
}

auto LockManager::CheckRowLock(Transaction *txn, table_oid_t oid, RID rid) -> std::pair<bool, LockMode> {
  if (txn->IsRowSharedLocked(oid, rid)) {  // 持有s锁
    return {true, LockMode::SHARED};
  }
  if (txn->IsRowExclusiveLocked(oid, rid)) {
    return {true, LockMode::EXCLUSIVE};
  }
  return {false, LockMode::SHARED};
}

auto LockManager::LockModeCompatible(LockMode left, LockMode right) -> bool {
  switch (left) {
    case LockMode::SHARED:
      return right == LockMode::SHARED || right == LockMode::INTENTION_SHARED;
    case LockMode::EXCLUSIVE:
      return false;
    case LockMode::INTENTION_EXCLUSIVE:
      return right == LockMode::INTENTION_EXCLUSIVE || right == LockMode::INTENTION_SHARED;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      return right == LockMode::INTENTION_SHARED;
    case LockMode::INTENTION_SHARED:
      return right != LockMode::EXCLUSIVE;
  }
}
auto LockManager::GrantLock(Transaction *txn, const std::shared_ptr<LockRequestQueue> &queue,
                            const std::shared_ptr<LockRequest> &request)
    -> bool {  // 查看是否request授予锁，reqeust之前的锁是否和先前的锁都兼容 // o(n2)
  auto beg = queue->request_queue_.begin();
  auto end = queue->request_queue_.end();
  auto request_it = beg;
  while (request_it != end && (*request_it) != request) {
    request_it++;
  }
  while (request_it != beg) {
    auto it = beg;
    while (it != end && it != request_it) {
      if (!LockModeCompatible((*it)->lock_mode_, (*request_it)->lock_mode_)) {
        return false;
      }
      it++;
    }
    request_it--;
  }
  return true;
}

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  txn_id_t txn_id = txn->GetTransactionId();
  CheckCompatible(txn, lock_mode);
  // txn, lock_mode, isolationlevel 兼容
  // 进入临界区之前提前检查，txn是否获得oid 的锁
  auto [need_upgrade, held_lock_mode] = CheckTableLock(txn, oid);
  if (need_upgrade) {                   // 已经持有锁
    if (held_lock_mode == lock_mode) {  // 申请相同的锁
      return true;
    }
    CheckLockUpgrade(txn, held_lock_mode, lock_mode);
    // 通过升级检查
  }
  table_lock_map_latch_.lock();  // 申请map的锁
  // LockTableInfo(txn, oid, lock_mode);
  if (table_lock_map_.find(oid) != table_lock_map_.end()) {
    auto queue = table_lock_map_[oid];
    std::unique_lock<std::mutex> lk(queue->latch_);  // 用未上锁的queue->latch_构造lk
    table_lock_map_latch_.unlock();                  // 释放 map的锁
    std::shared_ptr<LockRequest> request;
    if (need_upgrade) {
      if (queue->upgrading_ != INVALID_TXN_ID) {  // 已经有一个事务的锁正在升级
        txn->SetState(TransactionState::ABORTED);
        throw bustub::TransactionAbortException(txn_id, AbortReason::UPGRADE_CONFLICT);
        return false;
      }
      queue->upgrading_ = txn_id;  // 标记升级
      auto it = queue->request_queue_.begin();
      while (it != queue->request_queue_.end() && (*it)->txn_id_ != txn_id) {
        it++;
      }
      assert(it != queue->request_queue_.end());
      assert((*it)->lock_mode_ == held_lock_mode);

      UpdateTableLockSet(txn, oid, (*it)->lock_mode_, false);

      queue->request_queue_.erase(it);  // 删除这个 记录
      it = queue->request_queue_.begin();
      while (it != queue->request_queue_.end() && (*it)->granted_) {
        it++;
      }                                                                 // 找到第一个granted_为false的请求
      request = std::make_shared<LockRequest>(txn_id, lock_mode, oid);  // 新建一个请求节点

      queue->request_queue_.insert(it, request);                        // 新加入节点放在it之前
    } else {                                                            // 不需要升级
      request = std::make_shared<LockRequest>(txn_id, lock_mode, oid);  // 新建一个请求节点
      queue->request_queue_.emplace_back(request);                      // 新的请求放在末尾
    }

    while (!GrantLock(txn, queue, request)) {
      queue->cv_.wait(lk);
      if (txn->GetState() == TransactionState::ABORTED) {
        auto it = queue->request_queue_.begin();
        while (it != queue->request_queue_.end() && (*it) != request) {
          it++;
        }
        queue->request_queue_.erase(it);
        if (queue->upgrading_ == txn_id) {
          queue->upgrading_ = INVALID_TXN_ID;
        }
        queue->cv_.notify_all();
        return false;
      }
    }
    //  持有queue->latch_
    request->granted_ = true;
    if (queue->upgrading_ == txn_id) {
      queue->upgrading_ = INVALID_TXN_ID;
    }
    UpdateTableLockSet(txn, oid, lock_mode, true);

  } else {                                                                 // 没有相应的队列
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();           // 新建一个队列
    auto request = std::make_shared<LockRequest>(txn_id, lock_mode, oid);  // 新建一个请求节点

    request->granted_ = true;
    table_lock_map_[oid]->request_queue_.emplace_back(request);

    // 将获得的锁加入 事务的 lock set
    UpdateTableLockSet(txn, oid, lock_mode, true);
    table_lock_map_latch_.unlock();  // 释放 map的锁
  }
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  // 检查是否持有这个table的锁
  txn_id_t txn_id = txn->GetTransactionId();
  auto [held, held_lock_mode] = CheckTableLock(txn, oid);
  if (!held) {  // 没有持有该table的锁
    txn->SetState(TransactionState::ABORTED);
    throw bustub::TransactionAbortException(txn_id, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    return false;
  }
  auto s_row_lock_set = txn->GetSharedRowLockSet();
  auto x_row_lock_set = txn->GetExclusiveRowLockSet();
  // 检查 txn是否持有table中row的锁， 记得x_row_lock_set->erase();
  if (!(*s_row_lock_set)[oid].empty() || !(*x_row_lock_set)[oid].empty()) {
    txn->SetState(TransactionState::ABORTED);
    throw bustub::TransactionAbortException(txn_id, AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
    return false;
  }

  table_lock_map_latch_.lock();  // 获取map的锁
                                 // UnLockTableInfo(txn, oid);
  auto queue = table_lock_map_[oid];
  queue->latch_.lock();            // 获取queue的锁
  table_lock_map_latch_.unlock();  // 释放map的锁

  auto it = queue->request_queue_.begin();
  while (it != queue->request_queue_.end() && (*it)->txn_id_ != txn_id) {
    it++;
  }  // 找到相应的请求
  assert(it != queue->request_queue_.end());
  assert((*it)->granted_);

  LockMode lock_mode = (*it)->lock_mode_;
  assert(lock_mode == held_lock_mode);

  UpdateTableLockSet(txn, oid, lock_mode, false);  // bookkeeping
  queue->request_queue_.erase(it);                 // 删除相应的记录

  UpdateTxnState(txn, lock_mode);
  queue->latch_.unlock();
  queue->cv_.notify_all();
  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  // LockRowInfo(txn, oid, rid, lock_mode);
  txn_id_t txn_id = txn->GetTransactionId();

  CheckCompatible(txn, lock_mode);
  if (lock_mode != LockMode::SHARED && lock_mode != LockMode::EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    throw bustub::TransactionAbortException(txn_id, AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
    return false;
  }

  // 检查table 的锁
  auto [table_lock, table_lock_mode] = CheckTableLock(txn, oid);
  if (!table_lock) {  // table 没有锁
    txn->SetState(TransactionState::ABORTED);
    throw bustub::TransactionAbortException(txn_id, AbortReason::TABLE_LOCK_NOT_PRESENT);
    return false;
  }

  if (lock_mode == LockMode::EXCLUSIVE) {  // table锁不正确
    if (table_lock_mode == LockMode::SHARED || table_lock_mode == LockMode::INTENTION_SHARED) {
      txn->SetState(TransactionState::ABORTED);
      throw bustub::TransactionAbortException(txn_id, AbortReason::TABLE_LOCK_NOT_PRESENT);
      return false;
    }
  }
  auto [need_upgrade, held_lock_mode] = CheckRowLock(txn, oid, rid);
  if (need_upgrade) {  // 之前持有锁
    if (held_lock_mode == lock_mode) {
      return true;
    }
    CheckLockUpgrade(txn, held_lock_mode, lock_mode);
  }

  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) != row_lock_map_.end()) {  // 请求队列存在
    auto queue = row_lock_map_[rid];
    std::unique_lock<std::mutex> lk(queue->latch_);
    row_lock_map_latch_.unlock();
    std::shared_ptr<LockRequest> request;
    if (need_upgrade) {  // 需要升级
      if (queue->upgrading_ != INVALID_TXN_ID) {
        txn->SetState(TransactionState::ABORTED);
        throw bustub::TransactionAbortException(txn_id, AbortReason::UPGRADE_CONFLICT);
        return false;
      }

      queue->upgrading_ = txn_id;  // 标记 upgrade_
      // 这里手动删除锁
      auto it = queue->request_queue_.begin();
      while (it != queue->request_queue_.end() && (*it)->txn_id_ != txn_id) {
        it++;
      }
      assert(it != queue->request_queue_.end());
      assert((*it)->lock_mode_ == held_lock_mode);

      UpdateRowLockSet(txn, oid, rid, (*it)->lock_mode_, false);
      queue->request_queue_.erase(it);  // 删除这个 记录
      it = queue->request_queue_.begin();
      while (it != queue->request_queue_.end() && (*it)->granted_) {
        it++;
      }                                                                      // 找到第一个granted_为false的请求
      request = std::make_shared<LockRequest>(txn_id, lock_mode, oid, rid);  // 新建一个请求节点

      queue->request_queue_.insert(it, request);                             // 新加入节点放在it之前
    } else {                                                                 // 不需要升级
      request = std::make_shared<LockRequest>(txn_id, lock_mode, oid, rid);  // 建立新的请求
      queue->request_queue_.emplace_back(request);
    }

    while (!GrantLock(txn, queue, request)) {
      queue->cv_.wait(lk);
      if (txn->GetState() == TransactionState::ABORTED) {
        auto it = queue->request_queue_.begin();
        while (it != queue->request_queue_.end() && (*it) != request) {
          it++;
        }  // 找到request 这个请求
        queue->request_queue_.erase(it);
        if (queue->upgrading_ == txn_id) {
          queue->upgrading_ = INVALID_TXN_ID;
        }
        queue->cv_.notify_all();  // 这里需要吗
        return false;
      }
    }
    // 持有queue->latch_
    request->granted_ = true;
    if (queue->upgrading_ == txn_id) {
      queue->upgrading_ = INVALID_TXN_ID;
    }
    UpdateRowLockSet(txn, oid, rid, lock_mode, true);
  } else {  // 请求队列不存在
    row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
    auto request = std::make_shared<LockRequest>(txn_id, lock_mode, oid, rid);
    request->granted_ = true;
    row_lock_map_[rid]->request_queue_.emplace_back(request);

    // 更新row lock set
    UpdateRowLockSet(txn, oid, rid, lock_mode, true);
    row_lock_map_latch_.unlock();
  }
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  // UnLockRowInfo(txn, oid, rid);
  txn_id_t txn_id = txn->GetTransactionId();
  auto [held, held_lock_mode] = CheckRowLock(txn, oid, rid);
  if (!held) {
    txn->SetState(TransactionState::ABORTED);
    throw bustub::TransactionAbortException(txn_id, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  row_lock_map_latch_.lock();
  auto queue = row_lock_map_[rid];
  queue->latch_.lock();
  row_lock_map_latch_.unlock();

  auto it = queue->request_queue_.begin();
  while (it != queue->request_queue_.end() && (*it)->txn_id_ != txn_id) {
    it++;
  }  // 找到相应的请求
  assert((*it)->granted_);
  assert(it != queue->request_queue_.end());

  LockMode lock_mode = (*it)->lock_mode_;
  assert(lock_mode == held_lock_mode);

  UpdateRowLockSet(txn, oid, rid, lock_mode, false);  // bookkeeping
  queue->request_queue_.erase(it);                    // 删除相应的记录

  UpdateTxnState(txn, lock_mode);
  queue->latch_.unlock();
  queue->cv_.notify_all();
  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {  // t1 ----> t2
  waits_for_[t1].insert(t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  assert(waits_for_[t1].find(t2) != waits_for_[t1].end());
  waits_for_[t1].erase(t2);
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  std::unordered_map<txn_id_t, int> color;
  std::unordered_map<txn_id_t, int> parent;
  std::set<txn_id_t> txns;
  for (const auto &[txn, _] : waits_for_) {
    txns.insert(txn);
  }
  std::function<bool(int)> dfs = [&](txn_id_t s) -> bool {  // 从节点s出发是否有环
    color[s] = GRAY;
    for (const auto &next : waits_for_[s]) {
      if (color[next] == BLACK) {
        continue;
      }
      if (color[next] == GRAY) {  // next -->....s-->next
        txn_id_t max_txn = s;
        txn_id_t now = s;
        while (now != next) {
          now = parent[now];
          max_txn = std::max(max_txn, now);
        }
        *txn_id = max_txn;
        return true;
      }
      parent[next] = s;  // 表示s-->next有条路径
      if (dfs(next)) {
        return true;
      }
    }
    color[s] = BLACK;
    return false;
  };
  for (const auto &begtxn : txns) {
    for (const auto &txn : txns) {
      color[txn] = WHITE;
      parent[txn] = -1;
    }
    if (dfs(begtxn)) {
      return true;
    }
  }
  return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  for (const auto &[beg, vec] : waits_for_) {
    for (const auto &end : vec) {
      edges.emplace_back(beg, end);
    }
  }
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
      waits_for_latch_.lock();
      table_lock_map_latch_.lock();
      row_lock_map_latch_.lock();
      for (const auto &[oid, queue] : table_lock_map_) {  // 对于同一个oid资源
        queue->latch_.lock();
        std::vector<txn_id_t> granted_set;  // 所有获取锁的事务集合
        std::vector<txn_id_t> waiting_set;  // 所有等待锁的事务集合
        for (const auto &req : queue->request_queue_) {
          txn_id_t txn_id = req->txn_id_;
          if (req->granted_) {
            granted_set.emplace_back(txn_id);
          } else {
            waiting_set.emplace_back(txn_id);
            table_requesting_[txn_id].push_back(oid);  // 记录每个事务的正在等待的资源
          }
        }
        for (const auto &waiting_txn_id : waiting_set) {  // 添加边，建图
          for (const auto &granted_txn_id : granted_set) {
            AddEdge(waiting_txn_id, granted_txn_id);
          }
        }
        queue->latch_.unlock();
      }

      for (const auto &[rid, queue] : row_lock_map_) {  // 对于同一rid资源
        queue->latch_.lock();
        std::vector<txn_id_t> granted_set;  // 所有获取锁的事务集合
        std::vector<txn_id_t> waiting_set;  // 所有等待锁的事务集合
        for (const auto &req : queue->request_queue_) {
          txn_id_t txn_id = req->txn_id_;
          if (req->granted_) {
            granted_set.emplace_back(txn_id);
          } else {
            waiting_set.emplace_back(txn_id);
            row_requesting_[txn_id].push_back(rid);  // 记录每个事务的正在等待的资源
          }
        }
        for (const auto &waiting_txn_id : waiting_set) {  // 添加边，建图
          for (const auto &granted_txn_id : granted_set) {
            AddEdge(waiting_txn_id, granted_txn_id);
          }
        }
        queue->latch_.unlock();
      }

      txn_id_t toabort = 0;
      while (HasCycle(&toabort)) {
        // toabout 是要abort的事务id
        Transaction *txn = TransactionManager::GetTransaction(toabort);
        txn->SetState(TransactionState::ABORTED);
        // 删除边
        std::vector<txn_id_t> ends;
        for (const auto &end : waits_for_[toabort]) {
          ends.push_back(end);
        }
        for (const auto &end : ends) {
          RemoveEdge(toabort, end);
        }
        waits_for_.erase(toabort);

        // 唤醒阻塞的线程
        if (table_requesting_.find(toabort) != table_requesting_.end()) {
          table_oid_t requested_oid = *(table_requesting_[toabort].begin());
          table_requesting_.erase(toabort);
          table_lock_map_[requested_oid]->cv_.notify_all();
        }

        if (row_requesting_.find(toabort) != row_requesting_.end()) {
          RID requested_rid = *(row_requesting_[toabort].begin());
          row_requesting_.erase(toabort);
          row_lock_map_[requested_rid]->cv_.notify_all();
        }
      }

      waits_for_.clear();
      row_requesting_.clear();
      table_requesting_.clear();

      waits_for_latch_.unlock();
      table_lock_map_latch_.unlock();
      row_lock_map_latch_.unlock();
    }
  }
}

}  // namespace bustub
