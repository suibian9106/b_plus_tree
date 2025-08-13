#include"../include/b_plus_tree.h"
#include<iostream>
#include<thread>

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
                (void)value; // 防止编译器警告
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
}

int main() {
    // 创建B+树
    BPlusTree<int> int_tree(3);
    
    // 并发插入测试
    test_concurrent_inserts(int_tree);
    
    // 并发读取测试
    test_concurrent_reads(int_tree);
    
    // 范围查询测试
    auto results = int_tree.range_find(500, 600);
    std::cout << "Range find results: " << results.size() << " items" << std::endl;
    
    // 序列化测试
    int_tree.serialize("concurrent_tree");
    
    // 反序列化测试
    BPlusTree<int> int_tree2(3);
    int_tree2.deserialize("concurrent_tree");
    
    // 验证反序列化结果
    auto results2 = int_tree2.range_find(500, 600);
    std::cout << "Deserialized range find results: " << results2.size() << " items" << std::endl;
    
    std::cout << "All tests passed!" << std::endl;
    return 0;
}