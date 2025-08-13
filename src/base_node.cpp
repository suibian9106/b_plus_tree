#include"base_node.h"

template <typename Key>
BaseNode<Key>::BaseNode(bool is_leaf) : 
    is_leaf(is_leaf), size(0), parent(nullptr) {
    keys.reserve(1);
}

template <typename Key>
int BaseNode<Key>::find_index(const Key& key) const {
    auto it = std::lower_bound(keys.begin(), keys.begin() + size, key);
    return it - keys.begin();
}

template <typename Key>
bool BaseNode<Key>::is_overloaded(int order) const {
    return size > order;
}

template <typename Key>
bool BaseNode<Key>::is_underloaded(int order) const {
    return size < (order + 1) / 2;
}

template <typename Key>
bool BaseNode<Key>::is_safe(int order) const {
    return ((size < order) && (size > ((order + 1) / 2)));
}

// 显式实例化
template class BaseNode<int>;
template class BaseNode<std::string>;