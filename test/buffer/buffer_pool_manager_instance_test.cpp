//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance_test.cpp
//
// Identification: test/buffer/buffer_pool_manager_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"
#include <cstdio>
#include <iostream>
#include <random>
#include <string>

#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"

namespace bustub {

// NOLINTNEXTLINE
// Check whether pages containing terminal characters can be recovered
TEST(BufferPoolManagerInstanceTest, BinaryDataTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 10;
  const size_t k = 5;

  std::random_device r;
  std::default_random_engine rng(r());
  std::uniform_int_distribution<char> uniform_dist(0);

  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManagerInstance(buffer_pool_size, disk_manager, k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);  // free_list_[0,1,2,3,4,5,6,7,8,9]
  // page_id:[0]->frame_id [0]

  // Scenario: The buffer pool is empty. We should be able to create a new page.
  ASSERT_NE(nullptr, page0);
  EXPECT_EQ(0, page_id_temp);

  char random_binary_data[BUSTUB_PAGE_SIZE];
  // Generate random binary data
  for (char &i : random_binary_data) {
    i = uniform_dist(rng);
  }

  // Insert terminal characters both in the middle and at end
  random_binary_data[BUSTUB_PAGE_SIZE / 2] = '\0';
  random_binary_data[BUSTUB_PAGE_SIZE - 1] = '\0';

  // Scenario: Once we have a page, we should be able to read and write content.
  std::memcpy(page0->GetData(), random_binary_data, BUSTUB_PAGE_SIZE);
  EXPECT_EQ(0, std::memcmp(page0->GetData(), random_binary_data, BUSTUB_PAGE_SIZE));

  // Scenario: We should be able to create new pages until we fill up the buffer pool.
  for (size_t i = 1; i < buffer_pool_size; ++i) {  // free_list_ [1,2,3,4,5,6,7,8,9];
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  }
  // page_id:[0]->frame_id [0] unevictable
  // page_id:[1]->frame_id [1] unevictable
  // page_id:[2]->frame_id [2] unevictable
  // page_id:[3]->frame_id [3] unevictable
  // page_id:[4]->frame_id [4] unevictable
  // page_id:[5]->frame_id [5] unevictable
  // page_id:[6]->frame_id [6] unevictable
  // page_id:[7]->frame_id [7] unevictable
  // page_id:[8]->frame_id [8] unevictable
  // page_id:[9]->frame_id [9] unevictable

  // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
  for (size_t i = buffer_pool_size; i < buffer_pool_size * 2; ++i) {  // free_list_ is empty and replacer is empty
    EXPECT_EQ(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: After unpinning pages {0, 1, 2, 3, 4} we should be able to create 5 new pages
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(true, bpm->UnpinPage(i, true));
    bpm->FlushPage(i);
  }
  // page_id:[0]->frame_id [0]   evictable
  // page_id:[1]->frame_id [1]   evictable
  // page_id:[2]->frame_id [2]   evictable
  // page_id:[3]->frame_id [3]   evictable
  // page_id:[4]->frame_id [4]   evictable
  // page_id:[5]->frame_id [5] unevictable
  // page_id:[6]->frame_id [6] unevictable
  // page_id:[7]->frame_id [7] unevictable
  // page_id:[8]->frame_id [8] unevictable
  // page_id:[9]->frame_id [9] unevictable

  for (int i = 0; i < 5; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
    bpm->UnpinPage(page_id_temp, false);
  }
  // page_id:[10]->frame_id [0]   evictable
  // page_id:[11]->frame_id [1]   evictable
  // page_id:[12]->frame_id [2]   evictable
  // page_id:[13]->frame_id [3]   evictable
  // page_id:[14]->frame_id [4]   evictable
  // page_id:[5]->frame_id [5]  unevictable
  // page_id:[6]->frame_id [6]  unevictable
  // page_id:[7]->frame_id [7]  unevictable
  // page_id:[8]->frame_id [8]  unevictable
  // page_id:[9]->frame_id [9]  unevictable
  EXPECT_EQ(14, page_id_temp);
  // Scenario: We should be able to fetch the data we wrote a while ago.
  page0 = bpm->FetchPage(0);
  // 0 不在page_table_中
  EXPECT_EQ(0, memcmp(page0->GetData(), random_binary_data, BUSTUB_PAGE_SIZE));
  EXPECT_EQ(true, bpm->UnpinPage(0, true));

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");

  delete bpm;
  delete disk_manager;
}

// NOLINTNEXTLINE
TEST(BufferPoolManagerInstanceTest, SampleTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 10;
  const size_t k = 5;

  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManagerInstance(buffer_pool_size, disk_manager, k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);  // free_list_[0,1,2,3,4,5,6,7,8,9]
  std::cout << "pages is " << page0 << std::endl;
  // page_id:[0]->frame_id [0]
  // Scenario: The buffer pool is empty. We should be able to create a new page.
  ASSERT_NE(nullptr, page0);
  EXPECT_EQ(0, page_id_temp);

  // Scenario: Once we have a page, we should be able to read and write content.
  snprintf(page0->GetData(), BUSTUB_PAGE_SIZE, "Hello");
  EXPECT_EQ(0, strcmp(page0->GetData(), "Hello"));

  // Scenario: We should be able to create new pages until we fill up the buffer pool.
  for (size_t i = 1; i < buffer_pool_size; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
  for (size_t i = buffer_pool_size; i < buffer_pool_size * 2; ++i) {
    EXPECT_EQ(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: After unpinning pages {0, 1, 2, 3, 4} and pinning another 4 new pages,
  // there would still be one buffer page left for reading page 0.
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(true, bpm->UnpinPage(i, true));
  }
  // page_id:[0]->frame_id [0]   evictable
  // page_id:[1]->frame_id [1]   evictable
  // page_id:[2]->frame_id [2]   evictable
  // page_id:[3]->frame_id [3]   evictable
  // page_id:[4]->frame_id [4]   evictable
  // page_id:[5]->frame_id [5] unevictable
  // page_id:[6]->frame_id [6] unevictable
  // page_id:[7]->frame_id [7] unevictable
  // page_id:[8]->frame_id [8] unevictable
  // page_id:[9]->frame_id [9] unevictable
  for (int i = 0; i < 4; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  }
  // page_id:[10]->frame_id [0] unevictable
  // page_id:[11]->frame_id [1] unevictable
  // page_id:[12]->frame_id [2] unevictable
  // page_id:[13]->frame_id [3] unevictable
  // page_id:[4]->frame_id [4]   evictable
  // page_id:[5]->frame_id [5] unevictable
  // page_id:[6]->frame_id [6] unevictable
  // page_id:[7]->frame_id [7] unevictable
  // page_id:[8]->frame_id [8] unevictable
  // page_id:[9]->frame_id [9] unevictable

  // Scenario: We should be able to fetch the data we wrote a while ago.
  page0 = bpm->FetchPage(0);
  std::cout << "pages is " << page0 << std::endl;
  EXPECT_EQ(0, strcmp(page0->GetData(), "Hello"));
  // page_id:[10]->frame_id [0] unevictable
  // page_id:[11]->frame_id [1] unevictable
  // page_id:[12]->frame_id [2] unevictable
  // page_id:[13]->frame_id [3] unevictable
  // page_id:[0]->frame_id [4]  unevictable
  // page_id:[5]->frame_id [5] unevictable
  // page_id:[6]->frame_id [6] unevictable
  // page_id:[7]->frame_id [7] unevictable
  // page_id:[8]->frame_id [8] unevictable
  // page_id:[9]->frame_id [9] unevictable

  // Scenario: If we unpin page 0 and then make a new page, all the buffer pages should
  // now be pinned. Fetching page 0 should fail.
  EXPECT_EQ(true, bpm->UnpinPage(0, true));
  EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  EXPECT_EQ(nullptr, bpm->FetchPage(0));

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");

  delete bpm;
  delete disk_manager;
}

}  // namespace bustub
