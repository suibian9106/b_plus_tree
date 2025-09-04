#include <atomic>
#include <gtest/gtest.h>
#include <random>
#include <set>
#include <thread>

#include "../include/b_plus_tree.h"

// 测试基本插入和查找
TEST(BPlusTreeTest, InsertAndFind) {
    BPlusTree<int> tree(3);
    tree.insert(5, 100);
    tree.insert(3, 200);
    tree.insert(7, 300);

    EXPECT_EQ(tree.find(5), 100);
    EXPECT_EQ(tree.find(3), 200);
    EXPECT_EQ(tree.find(7), 300);
    EXPECT_EQ(tree.find(10), 0);  // 不存在的键
}

// 测试删除操作
TEST(BPlusTreeTest, Delete) {
    BPlusTree<int> tree(3);
    tree.insert(1, 100);
    tree.insert(2, 200);
    tree.insert(3, 300);
    tree.insert(4, 400);

    tree.remove(2);
    tree.print_tree();
    EXPECT_EQ(tree.find(2), 0);

    tree.remove(3);
    EXPECT_EQ(tree.find(3), 0);

    // 删除后应保留其他键
    EXPECT_EQ(tree.find(1), 100);
    EXPECT_EQ(tree.find(4), 400);
}

// 测试范围查找
TEST(BPlusTreeTest, RangeFind) {
    BPlusTree<int> tree(4);
    for (int i = 1; i <= 10; i++) {
        tree.insert(i, i * 100);
    }

    auto results = tree.range_find(3, 7);
    ASSERT_EQ(results.size(), 5);
    for (int i = 0; i < 5; i++) {
        EXPECT_EQ(results[i].first, i + 3);
        EXPECT_EQ(results[i].second, (i + 3) * 100);
    }
}

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

// 测试树结构完整性
TEST(BPlusTreeTest, TreeStructure) {
    BPlusTree<int> tree(3);
    const int N = 100;
    for (int i = 1; i <= N; i++) {
        tree.insert(i, i);
        // tree.print_tree();
    }

    // 验证所有键都存在
    for (int i = 1; i <= N; i++) {
        EXPECT_EQ(tree.find(i), i);
    }

    // 删除一半键
    for (int i = 1; i <= N; i += 2) {
        tree.remove(i);
    }

    // 验证删除结果
    for (int i = 1; i <= N; i++) {
        if (i % 2 == 1) {
            EXPECT_EQ(tree.find(i), 0);
        } else {
            EXPECT_EQ(tree.find(i), i);
        }
    }
}

// 插入查找性能测试
TEST(BPlusTreePerf, BulkInsert) {
    const int N = 100000;
    BPlusTree<int> tree(256);

    auto start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < N; ++i) {
        tree.insert(i, i * 10);
    }
    auto end = std::chrono::high_resolution_clock::now();

    std::cout << "Bulk insert " << N
              << " elements: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << "ms\n";

    start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < N; ++i) {
        tree.find(i);
    }
    end = std::chrono::high_resolution_clock::now();

    std::cout << "Bulk find " << N
              << " elements: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << "ms\n";
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
    tree.print_tree();
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

const int data_size = 1000000;

// 测试插入操作的吞吐量
TEST(BPlusTreePerformanceTest, InsertThroughput) {
    BPlusTree<int> tree(100);
    auto test_with_threads = [&](int num_threads) {
        std::vector<std::thread> threads;
        auto start = std::chrono::high_resolution_clock::now();

        for (int i = 0; i < num_threads; i++) {
            threads.emplace_back([&, i] {
                for (int j = i; j < data_size; j += num_threads) {
                    tree.insert(j, j * 10);
                }
            });
        }

        for (auto& t : threads) {
            t.join();
        }

        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> duration = end - start;
        double throughput = data_size / duration.count();

        std::cout << "Threads: " << num_threads << " | Time: " << duration.count() << "s"
                  << " | Throughput: " << throughput << " ops/s\n";
    };

    // 测试不同线程数
    test_with_threads(1);
    test_with_threads(2);
    test_with_threads(4);
    test_with_threads(8);
}

// 测试查找操作的吞吐量
TEST(BPlusTreePerformanceTest, FindThroughput) {
    // 先插入测试数据
    BPlusTree<int> tree(100);
    for (int i = 0; i < data_size; i++) {
        tree.insert(i, i * 10);
    }

    auto test_with_threads = [&](int num_threads) {
        std::vector<std::thread> threads;
        std::atomic<int> found_count(0);
        auto start = std::chrono::high_resolution_clock::now();

        for (int i = 0; i < num_threads; i++) {
            threads.emplace_back([&, i] {
                std::random_device rd;
                std::mt19937 gen(rd());
                std::uniform_int_distribution<> distrib(0, data_size - 1);

                for (int j = 0; j < data_size / num_threads; j++) {
                    int key = distrib(gen);
                    uint64_t value = tree.find(key);
                    if (value != 0) found_count++;
                }
            });
        }

        for (auto& t : threads) {
            t.join();
        }

        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> duration = end - start;
        double throughput = data_size / duration.count();

        std::cout << "Threads: " << num_threads << " | Time: " << duration.count() << "s"
                  << " | Throughput: " << throughput << " ops/s"
                  << " | Found: " << found_count << "/" << data_size << "\n";
    };

    test_with_threads(1);
    test_with_threads(2);
    test_with_threads(4);
    test_with_threads(8);
}

// 测试混合操作的吞吐量
TEST(BPlusTreePerformanceTest, MixedOperations) {
    BPlusTree<int> tree(100);
    const int num_ops = 500000;

    auto test_with_threads = [&](int num_threads, double insert_ratio, double delete_ratio) {
        std::vector<std::thread> threads;
        std::atomic<int> insert_count(0);
        std::atomic<int> delete_count(0);
        std::atomic<int> find_count(0);

        auto start = std::chrono::high_resolution_clock::now();

        for (int i = 0; i < num_threads; i++) {
            threads.emplace_back([&, i] {
                std::random_device rd;
                std::mt19937 gen(rd());
                std::uniform_int_distribution<> key_distrib(0, num_ops);
                std::uniform_real_distribution<> op_distrib(0.0, 1.0);

                for (int j = 0; j < num_ops / num_threads; j++) {
                    int key = key_distrib(gen);
                    double op = op_distrib(gen);

                    if (op < insert_ratio) {
                        tree.insert(key, key * 10);
                        insert_count++;
                    } else if (op < insert_ratio + delete_ratio) {
                        tree.remove(key);
                        delete_count++;
                    } else {
                        tree.find(key);
                        find_count++;
                    }
                }
            });
        }

        for (auto& t : threads) {
            t.join();
        }

        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> duration = end - start;
        double throughput = num_ops / duration.count();

        std::cout << "Threads: " << num_threads << " | Insert: " << (insert_ratio * 100) << "%"
                  << " | Delete: " << (delete_ratio * 100) << "%"
                  << " | Find: " << (1 - insert_ratio - delete_ratio) * 100 << "%"
                  << " | Time: " << duration.count() << "s"
                  << " | Throughput: " << throughput << " ops/s\n";
    };

    // 测试不同工作负载
    test_with_threads(4, 0.7, 0.1);  // 70% 插入, 10% 删除, 20% 查找
    test_with_threads(4, 0.5, 0.2);  // 50% 插入, 30% 删除, 30% 查找
    test_with_threads(4, 0.3, 0.1);  // 30% 插入, 10% 删除, 60% 查找

    // 测试不同线程数
    test_with_threads(1, 0.7, 0.1);
    test_with_threads(2, 0.7, 0.1);
    test_with_threads(4, 0.7, 0.1);
    test_with_threads(8, 0.7, 0.1);
}