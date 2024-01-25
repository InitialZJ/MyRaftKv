#ifndef SKIPLIST_H_
#define SKIPLIST_H_

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <ctime>
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
  int size();

 private:
  void get_key_value_from_string(const std::string &str, std::string *key,
                                 std::string *value);
  bool is_valid_string(const std::string &str);

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
    while (current->forward[i] != NULL &&
           current->forward[i]->get_key() < key) {
      current = current->forward[i];
    }
    update[i] = current;
  }

  current = current->forward[0];
  if (current != NULL && current->get_key() == key) {
    std::cout << "key: " << key << ", exists" << std::endl;
    _mtx.unlock();
    return 1;
  }

  if (current == NULL || current->get_key() != key) {
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

template <typename K, typename V>
void SkipList<K, V>::display_list() {
  std::cout << "\n***** Skip List*****"
            << "\n";
  for (int i = 0; i <= _skip_list_level; i++) {
    Node<K, V> *node = this->_header->forward[i];
    std::cout << "Level " << i << ": ";
    while (node != NULL) {
      std::cout << node->get_key() << ": " << node->get_value() << ";";
      node = node->forward[i];
    }
    std::cout << std::endl;
  }
  std::cout << "***** Skip List*****\n\n";
}

template <typename K, typename V>
std::string SkipList<K, V>::dump_file() {
  Node<K, V> *node = this->_header->forward[0];
  SkipListDump<K, V> dumper;
  while (node != NULL) {
    dumper.insert(*node);
    node = node->forward[0];
  }
  std::stringstream ss;
  boost::archive::text_oarchive oa(ss);
  oa << dumper;
  return ss.str();
}

template <typename K, typename V>
void SkipList<K, V>::load_file(const std::string &dumpStr) {
  if (dumpStr.empty()) {
    return;
  }
  SkipListDump<K, V> dumper;
  std::stringstream iss(dumpStr);
  boost::archive::text_iarchive ia(iss);
  ia >> dumper;
  for (int i = 0; i < dumper.keyDumpVt_.size(); ++i) {
    insert_element(dumper.keyDumpVt_[i], dumper.valDumpVt_[i]);
  }
}

template <typename K, typename V>
int SkipList<K, V>::size() {
  return _element_count;
}

template <typename K, typename V>
void SkipList<K, V>::get_key_value_from_string(const std::string &str,
                                               std::string *key,
                                               std::string *value) {
  if (!is_valid_string(str)) {
    return;
  }
  *key = str.substr(0, str.find(delimiter));
  *value = str.substr(str.find(delimiter) + 1, str.length() - key->size() - 1);
}

template <typename K, typename V>
bool SkipList<K, V>::is_valid_string(const std::string &str) {
  if (str.empty()) {
    return false;
  }
  if (str.find(delimiter) == std::string::npos) {
    return false;
  }
  return true;
}

template <typename K, typename V>
void SkipList<K, V>::delete_element(K key) {
  _mtx.lock();
  Node<K, V> *current = this->_header;
  Node<K, V> *update[_max_level + 1];
  memset(update, 0, sizeof(Node<K, V> *) * (_max_level + 1));

  for (int i = _skip_list_level; i >= 0; i--) {
    while (current->forward[i] != NULL &&
           current->forward[i]->get_key() < key) {
      current = current->forward[i];
    }
    update[i] = current;
  }

  current = current->forward[0];
  if (current != NULL && current->get_key() == key) {
    for (int i = 0; i <= _skip_list_level; i++) {
      if (update[i]->forward[i] != current) {
        break;
      }
      update[i]->forward[i] = current->forward[i];
    }

    while (_skip_list_level > 0 && _header->forward[_skip_list_level] == 0) {
      _skip_list_level--;
    }

    std::cout << "Successfully deleted key " << key << std::endl;
    delete current;
    _element_count--;
  }
  _mtx.unlock();
  return;
}

template <typename K, typename V>
void SkipList<K, V>::insert_set_element(K &key, V &value) {
  V oldValue;
  if (search_element(key, oldValue)) {
    delete_element(key);
  }
  insert_element(key, value);
}

template <typename K, typename V>
bool SkipList<K, V>::search_element(K key, V &value) {
  Node<K, V> *current = _header;
  for (int i = _skip_list_level; i >= 0; i--) {
    while (current->forward[i] && current->forward[i]->get_key() < key) {
      current = current->forward[i];
    }
  }

  current = current->forward[0];
  if (current != NULL && current->get_key() == key) {
    value = current->get_value();
    std::cout << "Found key: " << key << ", value: " << current->get_value()
              << std::endl;
    return true;
  }

  std::cout << "Not found key: " << key << std::endl;
  return false;
}

template <typename K, typename V>
void SkipListDump<K, V>::insert(const Node<K, V> &node) {
  keyDumpVt_.emplace_back(node.get_key());
  valDumpVt_.emplace_back(node.get_value());
}

template <typename K, typename V>
SkipList<K, V>::SkipList(int max_level) {
  this->_max_level = max_level;
  this->_skip_list_level = 0;
  this->_element_count = 0;

  K k;
  V v;
  this->_header = new Node<K, V>(k, v, _max_level);
  srand(time(nullptr));
}

template <typename K, typename V>
SkipList<K, V>::~SkipList() {
  if (_file_writer.is_open()) {
    _file_writer.close();
  }
  if (_file_reader.is_open()) {
    _file_reader.close();
  }

  auto tmp = _header;
  while (_header->forward[0]) {
    tmp = _header->forward[0];
    _header->forward[0] = tmp->forward[0];
    delete tmp;
  }
  delete _header;
}

template <typename K, typename V>
int SkipList<K, V>::get_random_level() {
  int k = 1;
  while (k < _max_level && rand() % 2) {
    k++;
  }
  return k;
}

#endif  // !SKIPLIST_H_
