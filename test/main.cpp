#include <atomic>
#include <iostream>
#include <random>
#include <set>
#include <thread>

#include "../include/b_plus_tree.h"

// 测试函数
void test_concurrent_inserts(BPlusTree<int>& tree) {
    std::vector<std::thread> threads;
    for (int i = 0; i < 10; ++i) {
        threads.emplace_back([&tree, i] {
            for (int j = 0; j < 10; ++j) {
                int key = i * 100 + j;
                tree.insert(key, key * 10);
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }
}

void test_concurrent_reads(BPlusTree<int>& tree) {
    std::vector<std::thread> threads;
    for (int i = 2; i < 5; ++i) {
        threads.emplace_back([&tree, i] {
            for (int j = 3; j < 6; ++j) {
                int key = i * 100 + j;
                uint64_t value = tree.find(key);
                std::cout << "find key:" << key << " value:" << value << std::endl;
                (void)value;  // 防止编译器警告
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }
}

int main() {
    while (true) {
        BPlusTree<int> tree(100);
        const int num_ops = 50000000;

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
    // // 创建B+树
    // BPlusTree<int> int_tree(3);

    // // 并发插入测试
    // test_concurrent_inserts(int_tree);

    // // 并发读取测试
    // test_concurrent_reads(int_tree);

    // // 范围查询测试
    // auto results = int_tree.range_find(500, 600);
    // std::cout << "Range find results: " << results.size() << " items" << std::endl;

    // // 序列化测试
    // int_tree.serialize("concurrent_tree");

    // // 反序列化测试
    // BPlusTree<int> int_tree2(3);
    // int_tree2.deserialize("concurrent_tree");

    // // 验证反序列化结果
    // auto results2 = int_tree2.range_find(500, 600);
    // std::cout << "Deserialized range find results: " << results2.size() << " items" << std::endl;

    // std::cout << "All tests passed!" << std::endl;
    return 0;
}