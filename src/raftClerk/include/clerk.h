#ifndef CLERK_H_
#define CLERK_H_

#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <cerrno>
#include <ctime>
#include <string>
#include <vector>

#include "MrpcConfig.h"
#include "kvServerRPC.pb.h"
#include "raftServerRpcUtil.h"

class Clerk {
 private:
  // 保存所有raft节点
  std::vector<std::shared_ptr<raftServerRpcUtil>> m_servers;
  std::string m_clientId;
  int m_requestId;
  int m_recentLeaderId;

  std::string Uuid() {
    return std::to_string(rand()) + std::to_string(rand()) +
           std::to_string(rand()) + std::to_string(rand());
  }

  void PutAppend(std::string key, std::string value, std::string op);

 public:
  void Init(std::string configFileName);
  std::string Get(std::string key);
  void Put(std::string key, std::string value);
  void Append(std::string key, std::string value);

  Clerk();
};

#endif  // !CLERK_H_
