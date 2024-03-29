#include "MrpcConfig.h"

#include <iostream>
#include <string>

void MrpcConfig::LoadConfigFile(const char* config_file) {
  FILE* pf = fopen(config_file, "r");
  if (pf == nullptr) {
    std::cout << config_file << " is invalid" << std::endl;
    exit(EXIT_FAILURE);
  }

  while (!feof(pf)) {
    // 三种情况：参考test.conf
    // 注释
    // 正确的配置项k=v
    // 去掉开头多余的空格
    char buf[512] = {0};
    fgets(buf, 512, pf);
    std::string src_buf(buf);
    // 去掉字符串前面的空格
    Trim(src_buf);
    // 判断#的注释
    if (src_buf[0] == '#' || src_buf.empty()) {
      continue;
    }
    int idx = src_buf.find('=');
    if (idx == -1) {
      continue;
    }
    std::string key;
    std::string value;
    key = src_buf.substr(0, idx);
    Trim(key);
    int endidx = src_buf.find('\n', idx);
    value = src_buf.substr(idx + 1, endidx - idx - 1);
    Trim(value);
    m_configure.insert({key, value});
  }
  fclose(pf);
}

std::string MrpcConfig::Load(const std::string& key) {
  auto it = m_configure.find(key);
  if (it == m_configure.end()) {
    return "";
  }
  return it->second;
}

void MrpcConfig::Trim(std::string src_buf) {
  int idx = src_buf.find_first_not_of(' ');
  if (idx != -1) {
    src_buf = src_buf.substr(idx, src_buf.size() - idx);
  }
  idx = src_buf.find_last_not_of(' ');
  if (idx != -1) {
    src_buf = src_buf.substr(0, idx + 1);
  }
}
