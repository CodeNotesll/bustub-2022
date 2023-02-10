/**
 * extendible_hash_test.cpp
 */

#include <memory>
#include <thread>  // NOLINT

#include "container/hash/extendible_hash_table.h"
#include "gtest/gtest.h"

namespace bustub {

TEST(ExtendibleHashTableTest, SampleTest) {
  auto table = std::make_unique<ExtendibleHashTable<int, std::string>>(2);

  table->Insert(1, "a");
  table->Insert(2, "b");
  std::string result;
  table->Find(2, result);
  EXPECT_EQ("b", result);
  EXPECT_EQ(2, table->GetNumBuckets());
  EXPECT_EQ(1, table->GetLocalDepth(0));
  table->Insert(3, "c");
  table->Insert(4, "d");
  EXPECT_EQ(2, table->GetNumBuckets());
  EXPECT_EQ(1, table->GetLocalDepth(0));
  EXPECT_EQ(1, table->GetLocalDepth(1));
  table->Insert(5, "e");
  EXPECT_EQ(2, table->GetGlobalDepth());
  EXPECT_EQ(3, table->GetNumBuckets());
  EXPECT_EQ(1, table->GetLocalDepth(0));
  EXPECT_EQ(2, table->GetLocalDepth(1));
  EXPECT_EQ(1, table->GetLocalDepth(2));
  EXPECT_EQ(2, table->GetLocalDepth(3));
  table->Find(2, result);
  EXPECT_TRUE(table->Find(2, result));
  EXPECT_EQ("b", result);
  EXPECT_FALSE(table->Find(6, result));
  table->Insert(6, "f");
  // table->Find(6, result);
  EXPECT_TRUE(table->Find(6, result));
  EXPECT_EQ("f", result);
  table->Insert(7, "g");
  table->Insert(8, "h");
  table->Find(2, result);
  EXPECT_TRUE(table->Find(2, result));
  EXPECT_EQ("b", result);
  table->Insert(9, "i");
  EXPECT_EQ(2, table->GetLocalDepth(0));
  EXPECT_EQ(3, table->GetLocalDepth(1));
  EXPECT_EQ(2, table->GetLocalDepth(2));
  EXPECT_EQ(2, table->GetLocalDepth(3));

  table->Find(9, result);
  EXPECT_EQ("i", result);
  EXPECT_FALSE(table->Find(10, result));
  table->Find(8, result);
  EXPECT_EQ("h", result);
  table->Find(7, result);
  EXPECT_EQ("g", result);
  table->Find(6, result);
  EXPECT_EQ("f", result);
  table->Find(5, result);
  EXPECT_EQ("e", result);
  table->Find(4, result);
  EXPECT_EQ("d", result);
  table->Find(3, result);
  EXPECT_EQ("c", result);
  table->Find(2, result);
  EXPECT_TRUE(table->Find(2, result));
  EXPECT_EQ("b", result);
  table->Find(1, result);
  EXPECT_EQ("a", result);
  EXPECT_FALSE(table->Find(10, result));

  EXPECT_TRUE(table->Remove(8));
  EXPECT_TRUE(table->Remove(4));
  EXPECT_TRUE(table->Remove(1));
  EXPECT_FALSE(table->Remove(20));
}

TEST(ExtendibleHashTableTest, ConcurrentInsertTest) {
  const int num_runs = 50;
  const int num_threads = 3;

  // Run concurrent test multiple times to guarantee correctness.
  for (int run = 0; run < num_runs; run++) {
    auto table = std::make_unique<ExtendibleHashTable<int, int>>(2);
    std::vector<std::thread> threads;
    threads.reserve(num_threads);

    for (int tid = 0; tid < num_threads; tid++) {
      threads.emplace_back([tid, &table]() { table->Insert(tid, tid); });
    }
    for (int i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    EXPECT_EQ(table->GetGlobalDepth(), 1);
    for (int i = 0; i < num_threads; i++) {
      int val;
      EXPECT_TRUE(table->Find(i, val));
      EXPECT_EQ(i, val);
    }
  }
}

}  // namespace bustub
