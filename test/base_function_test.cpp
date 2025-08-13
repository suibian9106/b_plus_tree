#include<gtest/gtest.h>
#include"../include/b_plus_tree.h"
#include<random>
#include<set>
#include<thread>

// 测试基本插入和查找
TEST(BPlusTreeTest, InsertAndFind) {
    BPlusTree<int> tree(3);
    tree.insert(5, 100);
    tree.insert(3, 200);
    tree.insert(7, 300);
    
    EXPECT_EQ(tree.find(5), 100);
    EXPECT_EQ(tree.find(3), 200);
    EXPECT_EQ(tree.find(7), 300);
    EXPECT_EQ(tree.find(10), 0); // 不存在的键
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

// 插入性能测试
TEST(BPlusTreePerf, BulkInsert) {
    const int N = 100000;
    BPlusTree<int> tree(256);
    
    auto start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < N; ++i) {
        tree.insert(i, i*10);
    }
    auto end = std::chrono::high_resolution_clock::now();
    
    std::cout << "Bulk insert " << N << " elements: " 
              << std::chrono::duration_cast<std::chrono::milliseconds>(end-start).count()
              << "ms\n";
}