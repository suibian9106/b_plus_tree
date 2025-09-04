#pragma once

#include <fstream>
#include <iostream>
#include <mutex>
#include <queue>
#include <shared_mutex>
#include <stack>
#include <thread>
#include <unordered_map>

#include "base_node.h"
#include "internal_node.h"
#include "leaf_node.h"

template <typename Key>
class BPlusTree {
   private:
    int order;
    BaseNode<Key>* root;
    LeafNode<Key>* head_leaf;
    mutable std::shared_mutex root_mutex;
    mutable std::shared_mutex tree_mutex;

    LeafNode<Key>* find_leaf(const Key& key, std::queue<BaseNode<Key>*>& unique_locked_parent,
                             bool for_write = false) const;
    void handle_split(BaseNode<Key>* node);
    void handle_underflow(BaseNode<Key>* node);
    void merge_nodes(InternalNode<Key>* parent, int left_index, bool is_leaf);

    void serialize_key(std::ofstream& file, const int& key);
    void serialize_key(std::ofstream& file, const std::string& key);
    Key deserialize_key(std::ifstream& file);

   public:
    BPlusTree(int order);
    ~BPlusTree();

    void insert(const Key& key, uint64_t value);
    void remove(const Key& key);
    uint64_t find(const Key& key) const;
    std::vector<std::pair<Key, uint64_t>> range_find(const Key& start, const Key& end) const;

    void serialize(const std::string& base_filename);
    void deserialize(const std::string& base_filename);

    void print_tree() const;
};