#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <mutex>
#include <queue>
#include <shared_mutex>
#include <stack>
#include <stdexcept>
#include <string>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <vector>

// B+树节点的基类
template <typename Key>
class BaseNode {
   public:
    bool is_leaf;
    int size;  // 当前节点中键的数量
    std::vector<Key> keys;
    BaseNode* parent;
    mutable std::shared_mutex mutex;  // 节点级别的互斥锁

    BaseNode(bool is_leaf) : is_leaf(is_leaf), size(0), parent(nullptr) {}
    virtual ~BaseNode() = default;

    // 在节点中查找键的位置（第一个大于等于key的位置）
    int find_index(const Key& key) const {
        auto it = std::lower_bound(keys.begin(), keys.begin() + size, key);
        return it - keys.begin();
    }

    // 检查节点是否过载（键数量超过阶数）
    bool is_overloaded(int order) const { return size > order; }

    // 检查节点是否欠载（键数量少于阶数的一半）
    bool is_underloaded(int order) const { return size < (order + 1) / 2; }

    // 检查节点是否安全（不会引起分裂或合并）
    bool is_safe(int order) const { return ((size < order) && (size > ((order + 1) / 2))); }

    // 纯虚函数，子类需要实现
    virtual void insert_in_node(const Key& key, uint64_t value, BaseNode* right_child, int order) = 0;
    virtual void remove_from_node(int index, int order) = 0;
};

// 叶子节点类
template <typename Key>
class LeafNode : public BaseNode<Key> {
   public:
    std::vector<uint64_t> values;
    LeafNode* prev;
    LeafNode* next;

    LeafNode() : BaseNode<Key>(true), prev(nullptr), next(nullptr) {
        this->keys.reserve(1);  // 预留空间
        values.reserve(1);
    }

    void insert_in_node(const Key& key, uint64_t value, BaseNode<Key>* right_child, int order) override {
        // 在叶子节点中插入键值对
        int index = this->find_index(key);
        if (index < this->size && this->keys[index] == key) {
            // 键已存在，更新值
            values[index] = value;
            return;
        }

        // 插入新键值对
        this->keys.insert(this->keys.begin() + index, key);
        values.insert(values.begin() + index, value);
        this->size++;
    }

    void remove_from_node(int index, int order) override {
        // 从叶子节点中删除键值对
        this->keys.erase(this->keys.begin() + index);
        values.erase(values.begin() + index);
        this->size--;
    }

    // 分裂叶子节点
    LeafNode* split(int order) {
        LeafNode* new_node = new LeafNode();
        int split_index = (this->size + 1) / 2;  // 分裂点

        // 移动后半部分键值到新节点
        new_node->keys.assign(this->keys.begin() + split_index, this->keys.end());
        new_node->values.assign(values.begin() + split_index, values.end());
        new_node->size = this->size - split_index;

        // 更新当前节点
        this->keys.resize(split_index);
        values.resize(split_index);
        this->size = split_index;

        // 更新叶子节点链表
        new_node->next = this->next;
        new_node->prev = this;
        if (this->next) this->next->prev = new_node;
        this->next = new_node;

        return new_node;
    }
};

// 内部节点类
template <typename Key>
class InternalNode : public BaseNode<Key> {
   public:
    std::vector<BaseNode<Key>*> children;

    InternalNode() : BaseNode<Key>(false) {
        this->keys.reserve(1);
        children.reserve(1);
    }

    ~InternalNode() {
        for (auto child : children) {
            delete child;
        }
    }

    void insert_in_node(const Key& key, uint64_t value, BaseNode<Key>* right_child, int order) override {
        // 在内部节点中插入键和子节点指针
        int index = this->find_index(key);
        this->keys.insert(this->keys.begin() + index, key);
        children.insert(children.begin() + index + 1, right_child);
        this->size++;
        right_child->parent = this;
    }

    void remove_from_node(int index, int order) override {
        // 从内部节点中删除键和子节点
        this->keys.erase(this->keys.begin() + index);
        children.erase(children.begin() + index + 1);
        this->size--;
    }

    // 分裂内部节点
    InternalNode* split(int order) {
        InternalNode* new_node = new InternalNode();
        int split_index = this->size / 2;  // 分裂点
        Key split_key = this->keys[split_index];

        // 移动后半部分键和子节点到新节点
        new_node->keys.assign(this->keys.begin() + split_index + 1, this->keys.end());
        new_node->children.assign(children.begin() + split_index + 1, children.end());
        new_node->size = this->size - split_index - 1;

        // 更新当前节点
        this->keys.resize(split_index);
        children.resize(split_index + 1);
        this->size = split_index;

        // 更新新节点子节点的父指针
        for (auto child : new_node->children) {
            child->parent = new_node;
        }

        return new_node;
    }

    // 从子节点借用键
    void borrow_from_left(int child_index, int order) {
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

    // 从右子节点借用键
    void borrow_from_right(int child_index, int order) {
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
};

// B+树主类
template <typename Key>
class BPlusTree {
   private:
    int order;  // B+树的阶
    BaseNode<Key>* root;
    LeafNode<Key>* head_leaf;              // 第一个叶子节点（用于范围查询）
    mutable std::mutex root_mutex;         // 保护根节点的互斥锁
    mutable std::shared_mutex tree_mutex;  // 保护整个树的读写锁,序列化和反序列化时

    // 递归查找叶子节点（带锁）
    LeafNode<Key>* find_leaf(const Key& key, std::queue<BaseNode<Key>*>& unique_locked_parent,
                             bool for_write = false) const {
        BaseNode<Key>* node = nullptr;
        BaseNode<Key>* parent = nullptr;

        {
            // 保护根节点访问
            std::lock_guard<std::mutex> lock(root_mutex);
            if (!root) return nullptr;
            node = root;
        }

        // 锁住当前节点
        if (for_write) {
            node->mutex.lock();
            unique_locked_parent.push(node);
        } else {
            node->mutex.lock_shared();
        }

        while (!node->is_leaf) {
            InternalNode<Key>* inode = static_cast<InternalNode<Key>*>(node);
            int index = inode->find_index(key);
            if (index < inode->size && inode->keys[index] == key) {
                index++;
            }

            BaseNode<Key>* child = inode->children[index];

            // 锁住子节点
            if (for_write) {
                child->mutex.lock();
                // 检查子节点是否安全，安全则释放祖先锁,从最上层的祖先节点开始释放,稍微提升并发性能
                if (for_write && child->is_safe(order)) {
                    while (!unique_locked_parent.empty()) {
                        parent = unique_locked_parent.front();
                        unique_locked_parent.pop();
                        parent->mutex.unlock();
                    }
                }
                unique_locked_parent.push(child);
            } else {
                parent = node;
                parent->mutex.unlock_shared();
                child->mutex.lock_shared();
            }

            // 移动到子节点
            node = child;
        }

        return static_cast<LeafNode<Key>*>(node);
    }

    // 插入后处理分裂
    void handle_split(BaseNode<Key>* node) {
        if (!node || !node->is_overloaded(order)) return;

        // 分裂节点
        BaseNode<Key>* new_node = nullptr;
        Key split_key;
        if (node->is_leaf) {
            LeafNode<Key>* leaf = static_cast<LeafNode<Key>*>(node);
            LeafNode<Key>* new_leaf = leaf->split(order);
            new_node = new_leaf;
            split_key = new_leaf->keys[0];
        } else {
            InternalNode<Key>* internal = static_cast<InternalNode<Key>*>(node);
            InternalNode<Key>* new_internal = internal->split(order);
            new_node = new_internal;
            split_key = internal->keys[internal->size];  // 分裂前中间键
        }

        // 处理根节点分裂
        {
            std::lock_guard<std::mutex> lock(root_mutex);
            if (node == root) {
                InternalNode<Key>* new_root = new InternalNode<Key>();
                new_root->keys.push_back(split_key);
                new_root->children.push_back(node);
                new_root->children.push_back(new_node);
                new_root->size = 1;

                // 更新根节点
                root = new_root;
                node->parent = root;
                new_node->parent = root;
                return;
            }
        }

        // 将新节点插入父节点
        InternalNode<Key>* parent = static_cast<InternalNode<Key>*>(node->parent);
        int index = parent->find_index(split_key);
        parent->insert_in_node(split_key, 0, new_node, order);
        handle_split(parent);  // 递归检查父节点
    }

    // 删除后处理下溢
    void handle_underflow(BaseNode<Key>* node) {
        if (!node || node == root || !node->is_underloaded(order)) return;

        InternalNode<Key>* parent = static_cast<InternalNode<Key>*>(node->parent);
        int child_index = -1;
        for (int i = 0; i < parent->children.size(); i++) {
            if (parent->children[i] == node) {
                child_index = i;
                break;
            }
        }
        if (child_index == -1) return;

        // 尝试从左兄弟借用
        if (child_index > 0) {
            BaseNode<Key>* left_sibling = parent->children[child_index - 1];
            if (left_sibling->size > (order + 1) / 2) {
                if (node->is_leaf) {
                    LeafNode<Key>* leaf = static_cast<LeafNode<Key>*>(node);
                    LeafNode<Key>* left_leaf = static_cast<LeafNode<Key>*>(left_sibling);

                    // 借用左兄弟的最后一个键值对
                    leaf->keys.insert(leaf->keys.begin(), left_leaf->keys.back());
                    leaf->values.insert(leaf->values.begin(), left_leaf->values.back());
                    leaf->size++;

                    left_leaf->keys.pop_back();
                    left_leaf->values.pop_back();
                    left_leaf->size--;

                    // 更新父节点键
                    parent->keys[child_index - 1] = leaf->keys[0];
                } else {
                    parent->borrow_from_left(child_index, order);
                }
                return;
            }
        }

        // 尝试从右兄弟借用
        if (child_index < parent->children.size() - 1) {
            BaseNode<Key>* right_sibling = parent->children[child_index + 1];
            if (right_sibling->size > (order + 1) / 2) {
                if (node->is_leaf) {
                    LeafNode<Key>* leaf = static_cast<LeafNode<Key>*>(node);
                    LeafNode<Key>* right_leaf = static_cast<LeafNode<Key>*>(right_sibling);

                    // 借用右兄弟的第一个键值对
                    leaf->keys.push_back(right_leaf->keys[0]);
                    leaf->values.push_back(right_leaf->values[0]);
                    leaf->size++;

                    right_leaf->keys.erase(right_leaf->keys.begin());
                    right_leaf->values.erase(right_leaf->values.begin());
                    right_leaf->size--;

                    // 更新父节点键
                    parent->keys[child_index] = right_leaf->keys[0];
                } else {
                    parent->borrow_from_right(child_index, order);
                }
                return;
            }
        }

        // 合并节点
        if (child_index > 0) {
            // 与左兄弟合并
            merge_nodes(parent, child_index - 1, node->is_leaf);
        } else {
            // 与右兄弟合并
            merge_nodes(parent, child_index, node->is_leaf);
        }

        // 递归检查父节点
        if (parent->is_underloaded(order) && parent != root) {
            handle_underflow(parent);
        } else if (parent == root && parent->size == 0) {
            // 根节点为空，更新根节点
            std::lock_guard<std::mutex> lock(root_mutex);
            root = parent->children[0];
            root->parent = nullptr;
            delete parent;
        }
    }

    // 合并节点
    void merge_nodes(InternalNode<Key>* parent, int left_index, bool is_leaf) {
        BaseNode<Key>* left = parent->children[left_index];
        BaseNode<Key>* right = parent->children[left_index + 1];

        if (is_leaf) {
            LeafNode<Key>* left_leaf = static_cast<LeafNode<Key>*>(left);
            LeafNode<Key>* right_leaf = static_cast<LeafNode<Key>*>(right);

            // 合并叶子节点
            left_leaf->keys.insert(left_leaf->keys.end(), right_leaf->keys.begin(), right_leaf->keys.end());
            left_leaf->values.insert(left_leaf->values.end(), right_leaf->values.begin(), right_leaf->values.end());
            left_leaf->size += right_leaf->size;

            // 更新叶子链表
            left_leaf->next = right_leaf->next;
            if (right_leaf->next) right_leaf->next->prev = left_leaf;

            // 删除右节点
            right_leaf->next = nullptr;
            right_leaf->prev = nullptr;
            delete right_leaf;
        } else {
            InternalNode<Key>* left_internal = static_cast<InternalNode<Key>*>(left);
            InternalNode<Key>* right_internal = static_cast<InternalNode<Key>*>(right);

            // 添加父节点中的键
            left_internal->keys.push_back(parent->keys[left_index]);

            // 合并键和子节点
            left_internal->keys.insert(left_internal->keys.end(), right_internal->keys.begin(),
                                       right_internal->keys.end());
            left_internal->children.insert(left_internal->children.end(), right_internal->children.begin(),
                                           right_internal->children.end());
            left_internal->size += right_internal->size + 1;

            // 更新子节点的父指针
            for (auto child : right_internal->children) {
                child->parent = left_internal;
            }

            // 删除右节点
            right_internal->children.clear();
            delete right_internal;
        }

        // 从父节点中删除键和子节点指针
        parent->remove_from_node(left_index, order);
    }

    // 序列化键（特化模板处理不同类型）
    void serialize_key(std::ofstream& file, const int& key) {
        file.write(reinterpret_cast<const char*>(&key), sizeof(key));
    }

    void serialize_key(std::ofstream& file, const std::string& key) {
        int32_t length = static_cast<int32_t>(key.size());
        file.write(reinterpret_cast<const char*>(&length), sizeof(length));
        file.write(key.c_str(), length);
    }

    // 反序列化键（特化模板处理不同类型）
    Key deserialize_key(std::ifstream& file) {
        if constexpr (std::is_same<Key, int>::value) {
            int key;
            file.read(reinterpret_cast<char*>(&key), sizeof(key));
            return key;
        } else if constexpr (std::is_same<Key, std::string>::value) {
            int32_t length;
            file.read(reinterpret_cast<char*>(&length), sizeof(length));
            std::string key(length, '\0');
            file.read(&key[0], length);
            return key;
        } else {
            static_assert(std::is_same<Key, int>::value || std::is_same<Key, std::string>::value,
                          "Unsupported key type");
            return Key();
        }
    }

   public:
    BPlusTree(int order) : order(order), root(nullptr), head_leaf(nullptr) {}

    ~BPlusTree() { delete root; }

    // 插入键值对（线程安全）
    void insert(const Key& key, uint64_t value) {
        std::shared_lock<std::shared_mutex> lock(tree_mutex);

        {
            //保护根节点
            std::lock_guard<std::mutex> lock(root_mutex);
            if (!root) {
                root = new LeafNode<Key>();
                head_leaf = static_cast<LeafNode<Key>*>(root);
            }
        }


        // 查找叶子节点并获取锁
        std::queue<BaseNode<Key>*> unique_locked_queue;  //加了写锁的祖先节点
        LeafNode<Key>* leaf = find_leaf(key, unique_locked_queue, true);

        // 插入操作
        leaf->insert_in_node(key, value, nullptr, order);

        // 处理分裂
        handle_split(leaf);
        // std::cout << std::this_thread::get_id() << std::endl

        // 释放锁
        // leaf->mutex.unlock();

        BaseNode<Key>* parent;
        while (!unique_locked_queue.empty()) {
            parent = unique_locked_queue.front();  //从最上层开始释放
            unique_locked_queue.pop();
            parent->mutex.unlock();
        }

        print_tree();

        // std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    // 删除键（线程安全）
    void remove(const Key& key) {
        std::shared_lock<std::shared_mutex> lock(tree_mutex);
        {
            std::lock_guard<std::mutex> lock(root_mutex);
            if (!root) return;
        }

        // 查找叶子节点并获取锁
        std::queue<BaseNode<Key>*> unique_locked_queue;  //加了写锁的祖先节点
        LeafNode<Key>* leaf = find_leaf(key, unique_locked_queue, true);

        int index = leaf->find_index(key);
        if (index >= leaf->size || leaf->keys[index] != key) {
            // 键不存在，释放锁
            // leaf->mutex.unlock();
            BaseNode<Key>* parent;
            while (!unique_locked_queue.empty()) {
                parent = unique_locked_queue.front();  //从最上层开始释放
                unique_locked_queue.pop();
                parent->mutex.unlock();
            }
            return;
        }

        // 删除操作
        leaf->remove_from_node(index, order);

        // 处理下溢
        handle_underflow(leaf);

        // 释放锁
        // leaf->mutex.unlock();
        BaseNode<Key>* parent;
        while (!unique_locked_queue.empty()) {
            parent = unique_locked_queue.front();  //从最上层开始释放
            unique_locked_queue.pop();
            parent->mutex.unlock();
        }
    }

    // 查找键对应的值（线程安全）
    uint64_t find(const Key& key) const {
        std::shared_lock<std::shared_mutex> lock(tree_mutex);

        if (!root) return 0;

        // 查找叶子节点并获取共享锁
        std::queue<BaseNode<Key>*> unique_locked_queue;  //加了写锁的祖先节点,无用
        LeafNode<Key>* leaf = find_leaf(key, unique_locked_queue, false);

        int index = leaf->find_index(key);
        uint64_t result = 0;
        if (index < leaf->size && leaf->keys[index] == key) {
            result = leaf->values[index];
        }

        // 释放锁
        leaf->mutex.unlock_shared();

        return result;
    }

    // 范围查找 [start, end]
    std::vector<std::pair<Key, uint64_t>> range_find(const Key& start, const Key& end) const {
        std::shared_lock<std::shared_mutex> lock(tree_mutex);

        std::vector<std::pair<Key, uint64_t>> results;
        {
            std::lock_guard<std::mutex> lock(root_mutex);
            if (!root) return results;
        }

        // 查找起始叶子节点并获取共享锁
        std::queue<BaseNode<Key>*> unique_locked_queue;  //加了写锁的祖先节点,无用
        LeafNode<Key>* current = find_leaf(start, unique_locked_queue, false);
        int start_index = 0;
        if (current) start_index = current->find_index(start);

        while (current) {
            // 锁住当前叶子节点
            // std::unique_lock<std::shared_mutex> current_lock(current->mutex);
            for (int i = start_index; i < current->size; i++) {
                if (current->keys[i] >= start && current->keys[i] <= end) {
                    results.push_back({current->keys[i], current->values[i]});
                } else if (current->keys[i] > end) {
                    // 释放当前锁并返回
                    current->mutex.unlock_shared();
                    return results;
                }
            }

            // 移动到下一个叶子节点
            LeafNode<Key>* next = current->next;

            // 释放当前锁
            current->mutex.unlock_shared();

            if (next) {
                // 锁住下一个节点
                next->mutex.lock_shared();
                start_index = 0;
                current = next;
            } else {
                current = nullptr;
            }
        }

        return results;
    }

    // 序列化到文件（线程安全）
    void serialize(const std::string& base_filename) {
        std::unique_lock<std::shared_mutex> lock(tree_mutex);

        std::ofstream header_file(base_filename + ".header", std::ios::binary);
        std::ofstream data_file(base_filename + ".data", std::ios::binary);

        if (!header_file || !data_file) {
            throw std::runtime_error("Failed to open files for serialization");
        }

        // 为每个节点分配唯一ID
        std::unordered_map<BaseNode<Key>*, int32_t> node_ids;
        int32_t next_id = 0;

        // 序列化元数据
        int32_t key_type = 0;  // 0:int 1:string
        int32_t root_id = -1;
        int32_t head_leaf_id = -1;

        if (std::is_same<Key, int>::value)
            key_type = 0;
        else if (std::is_same<Key, std::string>::value)
            key_type = 1;
        else
            throw std::runtime_error("Failed to serialize：Unknown Key Type");

        if (root) {
            // 分配节点ID（使用BFS遍历）
            std::queue<BaseNode<Key>*> q;
            q.push(root);
            node_ids[root] = next_id++;

            if (head_leaf) {
                node_ids[head_leaf] = next_id++;
            }

            while (!q.empty()) {
                BaseNode<Key>* node = q.front();
                q.pop();

                if (!node->is_leaf) {
                    InternalNode<Key>* inode = static_cast<InternalNode<Key>*>(node);
                    for (auto child : inode->children) {
                        if (node_ids.find(child) == node_ids.end()) {
                            node_ids[child] = next_id++;
                            q.push(child);
                        }
                    }
                }
            }

            root_id = node_ids[root];
            if (head_leaf) {
                head_leaf_id = node_ids[head_leaf];
            }
        }

        // 写入头文件
        header_file.write(reinterpret_cast<const char*>(&key_type), sizeof(key_type));
        header_file.write(reinterpret_cast<const char*>(&order), sizeof(order));
        header_file.write(reinterpret_cast<const char*>(&root_id), sizeof(root_id));
        header_file.write(reinterpret_cast<const char*>(&head_leaf_id), sizeof(head_leaf_id));

        // 写入节点数据（使用DFS遍历）
        if (root) {
            std::stack<BaseNode<Key>*> s;
            s.push(root);

            while (!s.empty()) {
                BaseNode<Key>* node = s.top();
                s.pop();

                int32_t node_id = node_ids[node];
                char node_type = node->is_leaf ? 1 : 0;

                // 写入节点ID和类型
                data_file.write(reinterpret_cast<const char*>(&node_id), sizeof(node_id));
                data_file.write(&node_type, sizeof(node_type));

                // 写入节点大小
                int32_t size = node->size;
                data_file.write(reinterpret_cast<const char*>(&size), sizeof(size));

                // 写入键
                for (int i = 0; i < size; i++) {
                    serialize_key(data_file, node->keys[i]);
                }

                if (node->is_leaf) {
                    LeafNode<Key>* leaf = static_cast<LeafNode<Key>*>(node);

                    // 写入值
                    for (int i = 0; i < size; i++) {
                        data_file.write(reinterpret_cast<const char*>(&leaf->values[i]), sizeof(uint64_t));
                    }

                    // 写入下一个叶子节点ID
                    int32_t next_leaf_id = leaf->next ? node_ids[leaf->next] : -1;
                    data_file.write(reinterpret_cast<const char*>(&next_leaf_id), sizeof(next_leaf_id));
                } else {
                    InternalNode<Key>* inode = static_cast<InternalNode<Key>*>(node);

                    // 写入子节点ID
                    for (int i = 0; i <= size; i++) {
                        int32_t child_id = node_ids[inode->children[i]];
                        data_file.write(reinterpret_cast<const char*>(&child_id), sizeof(child_id));
                    }

                    // 将子节点逆序压入堆栈，确保正确的反序列化顺序
                    for (int i = size; i >= 0; i--) {
                        s.push(inode->children[i]);
                    }
                }
            }
        }
    }

    // 从文件反序列化（线程安全）
    void deserialize(const std::string& base_filename) {
        std::unique_lock<std::shared_mutex> lock(tree_mutex);

        std::ifstream header_file(base_filename + ".header", std::ios::binary);
        std::ifstream data_file(base_filename + ".data", std::ios::binary);

        if (!header_file || !data_file) {
            throw std::runtime_error("Failed to open files for deserialization");
        }

        // 清除当前树
        delete root;
        root = nullptr;
        head_leaf = nullptr;

        // 读取头文件
        int32_t file_order, root_id, head_leaf_id, key_type;
        header_file.read(reinterpret_cast<char*>(&key_type), sizeof(key_type));
        header_file.read(reinterpret_cast<char*>(&file_order), sizeof(file_order));
        header_file.read(reinterpret_cast<char*>(&root_id), sizeof(root_id));
        header_file.read(reinterpret_cast<char*>(&head_leaf_id), sizeof(head_leaf_id));

        if (!(std::is_same<Key, int>::value && key_type == 0 ||
              std::is_same<Key, std::string>::value && key_type == 1)) {
            throw std::runtime_error("Failed to deserialize：Key Type Not Match");
        }

        order = file_order;

        // 如果没有根节点，直接返回
        if (root_id == -1) {
            return;
        }

        // 读取所有节点
        std::unordered_map<int32_t, BaseNode<Key>*> id_to_node;
        std::unordered_map<int32_t, int32_t> leaf_next_ids;
        std::unordered_map<int32_t, std::vector<int32_t>> internal_children_ids;

        while (true) {
            int32_t node_id;
            if (!data_file.read(reinterpret_cast<char*>(&node_id), sizeof(node_id))) {
                break;  // 文件结束
            }

            char node_type;
            data_file.read(&node_type, sizeof(node_type));

            int32_t size;
            data_file.read(reinterpret_cast<char*>(&size), sizeof(size));

            BaseNode<Key>* node = nullptr;

            if (node_type == 1) {  // 叶子节点
                LeafNode<Key>* leaf = new LeafNode<Key>();
                node = leaf;
                leaf->size = size;

                // 读取键
                for (int i = 0; i < size; i++) {
                    Key key = deserialize_key(data_file);
                    leaf->keys.push_back(key);
                }

                // 读取值
                for (int i = 0; i < size; i++) {
                    uint64_t value;
                    data_file.read(reinterpret_cast<char*>(&value), sizeof(value));
                    leaf->values.push_back(value);
                }

                // 读取下一个叶子节点ID
                int32_t next_leaf_id;
                data_file.read(reinterpret_cast<char*>(&next_leaf_id), sizeof(next_leaf_id));
                leaf_next_ids[node_id] = next_leaf_id;
            } else {  // 内部节点
                InternalNode<Key>* inode = new InternalNode<Key>();
                node = inode;
                inode->size = size;

                // 读取键
                for (int i = 0; i < size; i++) {
                    Key key = deserialize_key(data_file);
                    inode->keys.push_back(key);
                }

                // 读取子节点ID
                std::vector<int32_t> children_ids;
                for (int i = 0; i <= size; i++) {
                    int32_t child_id;
                    data_file.read(reinterpret_cast<char*>(&child_id), sizeof(child_id));
                    children_ids.push_back(child_id);
                }
                internal_children_ids[node_id] = children_ids;
            }

            id_to_node[node_id] = node;
        }

        // 建立节点间的关系
        for (auto& kv : id_to_node) {
            int32_t id = kv.first;
            BaseNode<Key>* node = kv.second;
            if (node->is_leaf) {
                LeafNode<Key>* leaf = static_cast<LeafNode<Key>*>(node);
                int32_t next_id = leaf_next_ids[id];

                if (next_id != -1 && id_to_node.find(next_id) != id_to_node.end()) {
                    leaf->next = static_cast<LeafNode<Key>*>(id_to_node[next_id]);
                    if (leaf->next) {
                        leaf->next->prev = leaf;
                    }
                }
            } else {
                InternalNode<Key>* inode = static_cast<InternalNode<Key>*>(node);
                const auto& children_ids = internal_children_ids[id];

                for (int32_t child_id : children_ids) {
                    if (id_to_node.find(child_id) != id_to_node.end()) {
                        BaseNode<Key>* child = id_to_node[child_id];
                        inode->children.push_back(child);
                        child->parent = inode;
                    }
                }
            }
        }

        // 设置根节点和头叶子节点
        if (id_to_node.find(root_id) != id_to_node.end()) {
            root = id_to_node[root_id];
        }

        if (head_leaf_id != -1 && id_to_node.find(head_leaf_id) != id_to_node.end()) {
            head_leaf = static_cast<LeafNode<Key>*>(id_to_node[head_leaf_id]);
        }
    }

    // 打印树结构（用于调试，非线程安全）
    void print_tree() const {
        if (!root) return;

        std::queue<BaseNode<Key>*> q;
        q.push(root);
        while (!q.empty()) {
            int level_size = q.size();
            for (int i = 0; i < level_size; i++) {
                BaseNode<Key>* node = q.front();
                q.pop();
                std::cout << "[";
                for (int j = 0; j < node->size; j++) {
                    std::cout << node->keys[j];
                    if (j < node->size - 1) std::cout << ",";
                }
                std::cout << "] ";

                if (!node->is_leaf) {
                    InternalNode<Key>* inode = static_cast<InternalNode<Key>*>(node);
                    for (auto child : inode->children) {
                        q.push(child);
                    }
                }
            }
            std::cout << std::endl;
        }
        std::cout << std::endl;
    }
};

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