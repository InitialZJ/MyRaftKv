#ifndef SKIPLIST_H_
#define SKIPLIST_H_

#include <cmath>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <mutex>
#include <vector>

#define STORE_FILE "store/dumpFile"

static std::string delimiter = ":";

template <typename K, typename V>
class Node {
 public:
  Node() {}

  Node(K k, V v, int);

  ~Node();

  K get_key() const;

  V get_value() const;

  void set_value(V);

  Node<K, V> **forward;

  int node_level;

 private:
  K key;
  V value;
};

template <typename K, typename V>
Node<K, V>::Node(const K k, const V v, int level) {
  this->key = k;
  this->value = v;
  this->node_level = level;

  // 数组每个都是指针
  this->forward = new Node<K, V> *[level + 1];
  memset(this->forward, 0, sizeof(Node<K, V> *) * (level + 1));
}

template <typename K, typename V>
Node<K, V>::~Node() {
  delete[] forward;
}

template <typename K, typename V>
K Node<K, V>::get_key() const {
  return key;
}

template <typename K, typename V>
V Node<K, V>::get_value() const {
  return value;
}

template <typename K, typename V>
void Node<K, V>::set_value(V value) {
  this->value = value;
}

template <typename K, typename V>
class SkipListDump {
 public:
  friend class boost::serialization::access;
  template <typename Archive>
  void serialize(Archive &ar, const unsigned int version) {
    ar & keyDumpVt_;
    ar & valDumpVt_;
  }
  std::vector<K> keyDumpVt_;
  std::vector<K> valDumpVt_;
  void insert(const Node<K, V> &node);
};

template <typename K, typename V>
class SkipList {
 public:
  SkipList(int);
  ~SkipList();
  int get_random_level();
  Node<K, V> *create_node(K, V, int);
  int insert_element(K, V);
  void display_list();
  bool search_element(K, V &value);
  void delete_element(K);
  void insert_set_element(K &, V &);
  std::string dump_file();
  void load_file(const std::string &dumpStr);
  // 递归删除节点
  void clear(Node<K, V> *);
  int size();

 private:
  void get_key_value_from_string(const std::string &str, std::string *key,
                                 std::string *value);
  bool is_valid_string(const std::stirng &str);

 private:
  int _max_level;

  int _skip_list_level;

  Node<K, V> *_header;

  std::ofstream _file_writer;
  std::ifstream _file_reader;

  int _element_count;

  std::mutex _mtx;
};

template <typename K, typename V>
Node<K, V> *SkipList<K, V>::create_node(const K k, const V v, int level) {
  Node<K, V> *n = new Node<K, V>(k, v, level);
  return n;
}

template <typename K, typename V>
int SkipList<K, V>::insert_element(const K key, const V value) {
  _mtx.lock();
  Node<K, V> *current = this->_header;
  Node<K, V> *update[_max_level + 1];
  memset(update, 0, sizeof(Node<K, V> *) * (_max_level + 1));

  for (int i = _skip_list_level; i >= 0; i--) {
    while (current->forward[i] != nullptr &&
           current->forward[i].get_key() < key) {
      current = current->forward[i];
    }
    update[i] = current;
  }

  current = current->forward[0];
  if (current != nullptr && current->get_key() == key) {
    std::cout << "key: " << key << ", exists" << std::endl;
    _mtx.unlock();
    return 1;
  }

  if (current == nullptr || current->get_key() != key) {
    int random_level = get_random_level();
    if (random_level > _skip_list_level) {
      for (int i = _skip_list_level + 1; i < random_level + 1; i++) {
        update[i] = _header;
      }
      _skip_list_level = random_level;
    }

    Node<K, V> *inserted_node = create_node(key, value, random_level);
    for (int i = 0; i <= random_level; i++) {
      inserted_node->forward[i] = update[i]->forward[i];
      update[i]->forward[i] = inserted_node;
    }
    std::cout << "Successfully inserted key: " << key << ", value: " << value
              << std::endl;
  }
  _mtx.unlock();
  return 0;
}

#endif  // !SKIPLIST_H_
