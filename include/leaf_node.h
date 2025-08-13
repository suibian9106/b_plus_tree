#pragma once

#include"base_node.h"

template <typename Key>
class LeafNode : public BaseNode<Key> {
public:
    std::vector<uint64_t> values;
    LeafNode* prev;
    LeafNode* next;

    LeafNode();
    void insert_in_node(const Key& key, uint64_t value, BaseNode<Key>* right_child, int order) override;
    void remove_from_node(int index, int order) override;
    LeafNode* split(int order);
};