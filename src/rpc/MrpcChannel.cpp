#include "MrpcChannel.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <cerrno>
#include <string>

#include "MrpcController.h"
#include "rpc_header.pb.h"
#include "util.h"

void MrpcChannel::CallMethod(const google::protobuf::MethodDescriptor* method,
                             google::protobuf::RpcController* controller,
                             const google::protobuf::Message* request,
                             google::protobuf::Message* response,
                             google::protobuf::Closure* done) {
  if (m_clientFd == -1) {
    std::string errMsg;
    bool rt = newConnect(m_ip.c_str(), m_port, &errMsg);
    if (!rt) {
      DPrintf("[func-MrpcChannel::CallMethod]重链接ip: {%s} port: {%d}失败",
              m_ip.c_str(), m_port);
      controller->SetFailed(errMsg);
      return;
    } else {
      DPrintf("[func-MrpcChannel::CallMethod]链接ip: {%s} port: {%d}成功",
              m_ip.c_str(), m_port);
    }
  }

  // 需要得到序列化后的字节流
  // 4个字节代表rpc_header长度
  src::RpcHeader header;
  // rpc_header包含service_name, method_name, args_len
  const google::protobuf::ServiceDescriptor* service = method->service();
  std::string service_name = service->name();
  std::string method_name = method->name();
  // args序列化，并得到长度
  uint32_t args_len = 0;
  std::string args_str;
  if (request->SerializeToString(&args_str)) {
    args_len = args_str.size();
  } else {
    controller->SetFailed("serialize request error!");
    return;
  }

  header.set_args_len(args_len);
  header.set_method_name(method_name);
  header.set_service_name(service_name);
  // rpc_header序列化
  std::string rpc_header_str;
  uint32_t rpc_header_len = 0;
  if (header.SerializeToString(&rpc_header_str)) {
    rpc_header_len = rpc_header_str.size();
  } else {
    controller->SetFailed("serialize rpc header error!");
  }
  std::string rpc_header_len_str;
  rpc_header_len_str.insert(0, std::string((char*)&rpc_header_len, 4));

  // 字符串都已经准备好
  std::string send_str{rpc_header_len_str + rpc_header_str + args_str};

  // 发送rpc请求
  // 失败会重新链接再发送，重新连接失败会直接return
  while (-1 == send(m_clientFd, send_str.c_str(), send_str.size(), 0)) {
    char errtxt[512] = {0};
    sprintf(errtxt, "send error! errno: %d", errno);
    std::cout << "尝试重新连接对方ip: " << m_ip << ", 端口: " << m_port
              << std::endl;
    close(m_clientFd);
    m_clientFd = -1;
    std::string errMsg;
    bool rt = newConnect(m_ip.c_str(), m_port, &errMsg);
    if (!rt) {
      controller->SetFailed(errMsg);
      return;
    }
  }

  // 接收响应
  char buf[1024] = {0};
  int recv_size = 0;
  if (-1 == (recv_size = recv(m_clientFd, buf, 1024, 0))) {
    close(m_clientFd);
    m_clientFd = -1;
    char errtxt[512] = {0};
    sprintf(errtxt, "recv error! errno: %d", errno);
    controller->SetFailed(errtxt);
    return;
  }

  // 反序列化
  if (!response->ParseFromArray(buf, recv_size)) {
    char errtxt[1050] = {0};
    sprintf(errtxt, "parse error! response_str: %s", buf);
    controller->SetFailed(errtxt);
    return;
  }
}

bool MrpcChannel::newConnect(const char* ip, uint16_t port,
                             std::string* errMsg) {
  int clientfd = socket(AF_INET, SOCK_STREAM, 0);
  if (-1 == clientfd) {
    char errtxt[512] = {0};
    sprintf(errtxt, "recv error! errno: %d", errno);
    m_clientFd = -1;
    *errMsg = errtxt;
    return;
  }

  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(port);
  server_addr.sin_addr.s_addr = inet_addr(ip);
  // 连接rpc服务节点
  if (-1 ==
      connect(clientfd, (struct sockaddr*)&server_addr, sizeof(server_addr))) {
    close(clientfd);
    char errtxt[512] = {0};
    sprintf(errtxt, "connect fail! errno: %d", errno);
    m_clientFd = -1;
    *errMsg = errtxt;
    return false;
  }
  m_clientFd = clientfd;
  return true;
}

MrpcChannel::MrpcChannel(std::string ip, short port, bool connetNow)
    : m_ip(ip), m_port(port), m_clientFd(-1) {
  if (!connetNow) {
    return;
  }
  std::string errMsg;
  auto rt = newConnect(ip.c_str(), port, &errMsg);
  int tryCount = 3;
  while (!rt && tryCount--) {
    std::cout << errMsg << std::endl;
    rt = newConnect(ip.c_str(), port, &errMsg);
  }
}
