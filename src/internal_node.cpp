#include"internal_node.h"

template <typename Key>
InternalNode<Key>::InternalNode() : BaseNode<Key>(false) {
    this->children.reserve(1);
}

template <typename Key>
InternalNode<Key>::~InternalNode() {
    for (auto child : children) {
        delete child;
    }
}

template <typename Key>
void InternalNode<Key>::insert_in_node(const Key& key, uint64_t value, 
                                      BaseNode<Key>* right_child, int order) {
    int index = this->find_index(key);
    this->keys.insert(this->keys.begin() + index, key);
    children.insert(children.begin() + index + 1, right_child);
    this->size++;
    right_child->parent = this;
}

template <typename Key>
void InternalNode<Key>::remove_from_node(int index, int order) {
    this->keys.erase(this->keys.begin() + index);
    children.erase(children.begin() + index + 1);
    this->size--;
}

template <typename Key>
InternalNode<Key>* InternalNode<Key>::split(int order) {
    InternalNode* new_node = new InternalNode();
    int split_index = this->size / 2;
    Key split_key = this->keys[split_index];

    new_node->keys.assign(this->keys.begin() + split_index + 1, this->keys.end());
    new_node->children.assign(children.begin() + split_index + 1, children.end());
    new_node->size = this->size - split_index - 1;

    this->keys.resize(split_index);
    children.resize(split_index + 1);
    this->size = split_index;

    for (auto child : new_node->children) {
        child->parent = new_node;
    }

    return new_node;
}

template <typename Key>
void InternalNode<Key>::borrow_from_left(int child_index, int order) {
    BaseNode<Key>* child = children[child_index];
    BaseNode<Key>* left_sibling = children[child_index - 1];
    
    // 将左兄弟的最后一个键上移到父节点
    child->keys.insert(child->keys.begin(), this->keys[child_index - 1]);
    this->keys[child_index - 1] = left_sibling->keys[left_sibling->size - 1];
    
    // 移动左兄弟的最后一个子节点
    if (!child->is_leaf) {
        InternalNode* internal_child = static_cast<InternalNode*>(child);
        InternalNode* internal_left = static_cast<InternalNode*>(left_sibling);
        internal_child->children.insert(internal_child->children.begin(), 
                                        internal_left->children[internal_left->size]);
        internal_child->children[0]->parent = child;
        internal_left->children.pop_back();
    }
    
    left_sibling->keys.pop_back();
    left_sibling->size--;
    child->size++;
}

template <typename Key>
void InternalNode<Key>::borrow_from_right(int child_index, int order) {
    BaseNode<Key>* child = children[child_index];
    BaseNode<Key>* right_sibling = children[child_index + 1];
    
    // 将右兄弟的第一个键上移到父节点
    child->keys.push_back(this->keys[child_index]);
    this->keys[child_index] = right_sibling->keys[0];
    right_sibling->keys.erase(right_sibling->keys.begin());
    
    // 移动右兄弟的第一个子节点
    if (!child->is_leaf) {
        InternalNode* internal_child = static_cast<InternalNode*>(child);
        InternalNode* internal_right = static_cast<InternalNode*>(right_sibling);
        internal_child->children.push_back(internal_right->children[0]);
        internal_child->children.back()->parent = child;
        internal_right->children.erase(internal_right->children.begin());
    }
    
    right_sibling->size--;
    child->size++;
}

// 显式实例化
template class InternalNode<int>;
template class InternalNode<std::string>;