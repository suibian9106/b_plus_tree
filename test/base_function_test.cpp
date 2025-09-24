#include <atomic>
#include <gtest/gtest.h>
#include <random>
#include <set>
#include <thread>

#include "../include/b_plus_tree.h"

// 测试序列化与反序列化
TEST(BPlusTreeTest, Serialization) {
    BPlusTree<int> tree(3);
    tree.insert(10, 1000);
    tree.insert(20, 2000);
    tree.insert(30, 3000);

    tree.serialize("test_tree");

    BPlusTree<int> restored_tree(3);
    restored_tree.deserialize("test_tree");

    EXPECT_EQ(restored_tree.find(10), 1000);
    EXPECT_EQ(restored_tree.find(20), 2000);
    EXPECT_EQ(restored_tree.find(30), 3000);
}

// 测试字符串键类型
TEST(BPlusTreeTest, StringKeys) {
    BPlusTree<std::string> tree(3);
    tree.insert("apple", 1);
    tree.insert("banana", 2);
    tree.insert("orange", 3);

    EXPECT_EQ(tree.find("banana"), 2);
    EXPECT_EQ(tree.find("pear"), 0);

    tree.remove("apple");
    EXPECT_EQ(tree.find("apple"), 0);
}

// 测试并发插入
TEST(BPlusTreeConcurrencyTest, ConcurrentInsert) {
    BPlusTree<int> tree(4);
    const int num_threads = 8;
    const int num_per_thread = 10;
    std::vector<std::thread> threads;

    for (int i = 0; i < num_threads; i++) {
        threads.emplace_back([&, i] {
            for (int j = 0; j < num_per_thread; j++) {
                int key = i * num_per_thread + j;
                tree.insert(key, key * 10);
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // 验证所有键都存在
    for (int i = 0; i < num_threads * num_per_thread; i++) {
        uint64_t value = tree.find(i);
        ASSERT_EQ(value, i * 10);
    }
    // tree.print_tree();
}

// 测试并发插入和查找
TEST(BPlusTreeConcurrencyTest, ConcurrentInsertAndFind) {
    BPlusTree<int> tree(4);
    const int num_threads = 8;
    const int num_ops = 500;
    std::vector<std::thread> threads;
    std::atomic<int> found_count(0);

    for (int i = 0; i < num_threads; i++) {
        threads.emplace_back([&, i] {
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<> distrib(0, num_ops * num_threads);

            for (int j = 0; j < num_ops; j++) {
                int key = distrib(gen);

                if (j % 2 == 0) {
                    // 插入操作
                    tree.insert(key, key * 10 + 1);
                } else {
                    // 查找操作
                    uint64_t value = tree.find(key);
                    if (value != 0) found_count++;
                }
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // 验证树结构完整
    for (int i = 0; i < 1000; i++) {
        uint64_t value = tree.find(i);
        if (value != 0) {
            ASSERT_EQ(value, i * 10 + 1);
        }
    }
}

// 测试并发删除
TEST(BPlusTreeConcurrencyTest, ConcurrentDelete) {
    BPlusTree<int> tree(4);
    // 先插入测试数据
    for (int i = 0; i < 400; i++) {
        tree.insert(i, i * 10);
    }

    const int num_threads = 4;
    std::vector<std::thread> threads;

    for (int i = 0; i < num_threads; i++) {
        threads.emplace_back([&, i] {
            for (int j = i * 100; j < (i + 1) * 100; j += 2) {
                tree.remove(j);
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // 验证删除结果
    for (int i = 0; i < 400; i++) {
        uint64_t value = tree.find(i);
        if (i % 2 == 0) {
            ASSERT_EQ(value, 0);
        } else {
            ASSERT_EQ(value, i * 10);
        }
    }
}

// 测试范围查询的并发性
TEST(BPlusTreeConcurrencyTest, ConcurrentRangeQuery) {
    BPlusTree<int> tree(4);
    // 插入测试数据
    for (int i = 0; i < 1000; i++) {
        tree.insert(i, i * 10);
    }

    const int num_threads = 4;
    std::vector<std::thread> threads;
    std::atomic<int> total_results(0);

    for (int i = 0; i < num_threads; i++) {
        threads.emplace_back([&, i] {
            int start = i * 100;
            int end = start + 200;
            auto results = tree.range_find(start, end);
            total_results += results.size();

            for (const auto& [key, value] : results) {
                ASSERT_GE(key, start);
                ASSERT_LE(key, end);
                ASSERT_EQ(value, key * 10);
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    ASSERT_GT(total_results, 0);
}