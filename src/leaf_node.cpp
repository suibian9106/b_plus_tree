#include"leaf_node.h"

template <typename Key>
LeafNode<Key>::LeafNode() : BaseNode<Key>(true), prev(nullptr), next(nullptr) {
    this->values.reserve(1);
}

template <typename Key>
void LeafNode<Key>::insert_in_node(const Key& key, uint64_t value, 
                                  BaseNode<Key>* right_child, int order) {
    int index = this->find_index(key);
    if (index < this->size && this->keys[index] == key) {
        values[index] = value;
        return;
    }

    this->keys.insert(this->keys.begin() + index, key);
    values.insert(values.begin() + index, value);
    this->size++;
}

template <typename Key>
void LeafNode<Key>::remove_from_node(int index, int order) {
    this->keys.erase(this->keys.begin() + index);
    values.erase(values.begin() + index);
    this->size--;
}

template <typename Key>
LeafNode<Key>* LeafNode<Key>::split(int order) {
    LeafNode* new_node = new LeafNode();
    int split_index = (this->size + 1) / 2;

    new_node->keys.assign(this->keys.begin() + split_index, this->keys.end());
    new_node->values.assign(values.begin() + split_index, values.end());
    new_node->size = this->size - split_index;

    this->keys.resize(split_index);
    values.resize(split_index);
    this->size = split_index;

    new_node->next = this->next;
    new_node->prev = this;
    if (this->next) this->next->prev = new_node;
    this->next = new_node;

    return new_node;
}

// 显式实例化
template class LeafNode<int>;
template class LeafNode<std::string>;