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
    auto test_with_threads = [&](int num_threads, double insert_ratio, double delete_ratio) {
        std::vector<std::thread> threads;
        BPlusTree<int> tree(100);
        const int num_ops = 10000000;
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
                    } else if (op < insert_ratio + delete_ratio) {
                        tree.remove(key);
                    } else {
                        tree.find(key);
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

    // 测试不同线程数
    test_with_threads(1, 0.3, 0.05);
    test_with_threads(2, 0.3, 0.05);
    return 0;
}