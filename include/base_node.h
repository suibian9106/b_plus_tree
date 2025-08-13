#pragma once

#include<vector>
#include<shared_mutex>

template <typename Key>
class BaseNode {
public:
    bool is_leaf;
    int size;
    std::vector<Key> keys;
    BaseNode* parent;
    mutable std::shared_mutex mutex;

    BaseNode(bool is_leaf);
    virtual ~BaseNode() = default;

    int find_index(const Key& key) const;
    bool is_overloaded(int order) const;
    bool is_underloaded(int order) const;
    bool is_safe(int order) const;

    virtual void insert_in_node(const Key& key, uint64_t value, BaseNode* right_child, int order) = 0;
    virtual void remove_from_node(int index, int order) = 0;
};