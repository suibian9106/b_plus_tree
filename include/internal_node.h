#pragma once

#include"base_node.h"

template <typename Key>
class InternalNode : public BaseNode<Key> {
public:
    std::vector<BaseNode<Key>*> children;

    InternalNode();
    ~InternalNode();
    
    void insert_in_node(const Key& key, uint64_t value, BaseNode<Key>* right_child, int order) override;
    void remove_from_node(int index, int order) override;
    InternalNode* split(int order);
    
    void borrow_from_left(int child_index, int order);
    void borrow_from_right(int child_index, int order);
};