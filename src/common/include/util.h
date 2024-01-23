#ifndef UTIL_H_
#define UTIL_H_

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/access.hpp>
#include <condition_variable>
#include <functional>
#include <iostream>
#include <mutex>
#include <queue>
#include <random>
#include <sstream>
#include <thread>

#include "config.h"

template <typename F>
class DeferClass {
 public:
  DeferClass(F&& f) : m_func(std::forwawrd<F>(f)) {}
  DeferClass(const F& f) : m_func(f) {}
  ~DeferClass() { m_func(); }

  DeferClass(const DeferClass& e) = delete;
  DeferClass& operator=(const DeferClass& e) = delete;

 private:
  F m_func;
};

#define _CONCAT(a, b) a##b
#define _MAKE_DEFER_(line) DeferClass _CONCAT(defer_placeholder, line) = [&]()

#undef DEFER
#define DEFER _MAKE_DEFER_(__LINE__)

void DPrintf(const char* format, ...);

void myAssert(bool condition, std::string message = "Assertion failed!");

/*
您提供的代码是一个变长模板函数名为`format`。它接受一个格式字符串(`format_str`)和可变参数列表(`args`)，并返回一个格式化后的字符串。

函数的工作原理如下：

1. 它使用了变长模板参数(`Args...`)来接受不定数量的参数。

2. 函数内部使用了一个`std::stringstream`对象`ss`来构建最终的格式化字符串。

3. `int _[] = {((ss << args), 0)...};`
这一行代码使用了折叠表达式和逗号运算符来遍历可变参数列表。对于每个参数
`args`，它将参数插入到 `ss`
中，然后将结果与零进行逗号运算，这样整个表达式的结果是一个包含多个零的数组。

4. `(void)_;` 这一行代码用于抑制编译器对未使用变量`_`的警告。

5. `return ss.str();` 返回`ss`对象转换为字符串的结果，即最终的格式化字符串。

通过这个函数，您可以将参数按照指定的格式插入到格式字符串中，并得到最终的格式化结果。
*/
template <typename... Args>
std::string format(const char* format_str, Args... args) {
  std::stringstream ss;
  int _[] = {((ss << args), 0)...};
  (void)_;
  return ss.str();
}

std::chrono::_V2::system_clock::time_point now();

std::chrono::milliseconds getRandomizedElectionTimeout();
void sleepNMilliseconds(int N);

// 异步写日志的日志队列
template <typename T>
class LockQueue {
 public:
  void Push(const T& data) {
    std::lock_guard<std::mutex> lock(m_mutex);
    m_queue.push(data);
    m_condvariable.notify_one();
  }

  T Pop() {
    std::unique_lock<std::mutex> lock(m_mutex);
    while (m_queue.empty()) {
      m_condvariable.wait(lock);
    }
    T data = m_queue.front();
    m_queue.pop();
    return data;
  }

  bool timeOutPop(int timeout, T* ResData) {
    std::unique_lock<std::mutex> lock(m_mutex);

    // 获取当前时间点，并计算超时时刻
    auto now = std::chrono::system_clock::now();
    auto timeout_time = now + std::chrono::milliseconds(timeout);

    // 在超时之前，不断检查队列是否为空
    while (m_queue.empty()) {
      if (m_condvariable.wait_until(lock, timeout_time) ==
          std::cv_status::timeout) {
        return false;
      } else {
        continue;
      }
    }

    T data = m_queue.front();
    m_queue.pop();
    *ResData = data;
    return true;
  }

 private:
  std::queue<T> m_queue;
  std::mutex m_mutex;
  std::condition_variable m_condvariable;
};

// 这个Op是kv传递给raft的command
class Op {
 public:
  std::string Operation;  // "Get" "Put" "Append"
  std::string Key;
  std::string Value;
  std::string ClientId;
  int RequestId;  // 客户端号码请求的Request序列号，为了保证线性一致性

 public:
  std::string asString() const {
    std::stringstream ss;
    boost::archive::text_oarchive oa(ss);
    oa << *this;
    return ss.str();
  }

  bool parseFromString(std::string str) {
    std::stringstream iss(str);
    boost::archive::text_iarchive ia(iss);
    ia >> *this;
    return true;
  }

 public:
  friend std::ostream& operator<<(std::ostream& os, const Op& obj) {
    os << "[MyClass::Operator{" + obj.Operation + "}, Key{" + obj.Key +
              "}, Value{" + obj.Value + "}, ClientId{" + obj.ClientId +
              "}, RequestId{" + std::to_string(obj.RequestId) + "}]";
    return os;
  }

 private:
  friend class boost::serialization::access;
  template <typename Archive>
  void serialize(Archive& ar, const unsigned int version) {
    // 使用 & 操作符进行序列化
    ar & Operation;
    ar & Key;
    ar & Value;
    ar & ClientId;
    ar & RequestId;
  }
};

const std::string OK = "OK";
const std::string ErrNoKey = "ErrNoKey";
const std::string ErrWrongLeader = "ErrWrongLeader";

bool isReleasePort(unsigned short usPort);

bool getReleasePort(short& port);

#endif  // !UTIL_H_
