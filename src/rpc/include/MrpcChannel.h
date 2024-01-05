#ifndef MRPC_CHANNEL_H_
#define MRPC_CHANNEL_H_

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <google/protobuf/service.h>

#include <algorithm>
#include <functional>
#include <iostream>
#include <map>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

// 真正负责发送和接收的前后处理工作
// 如消息的组织方式，向哪个节点发送等等
class MrpcChannel : public google::protobuf::RpcChannel {
 public:
  void CallMethod(const google::protobuf::MethodDescriptor* method,
                  google::protobuf::RpcController* controller,
                  const google::protobuf::Message* request,
                  google::protobuf::Message* response,
                  google::protobuf::Closure* done) override;

  MrpcChannel(std::string ip, short port, bool connectNow);

private:
	int m_clientFd;
	const std::string m_ip;
	const uint16_t m_port;
	bool newConnect(const char* ip, uint16_t port, std::string* errMsg);
};

#endif  // !MRPC_CHANNEL_H_
