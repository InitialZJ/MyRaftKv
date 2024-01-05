#ifndef RPC_PROVIDER_H_
#define RPC_PROVIDER_H_

#include <google/protobuf/descriptor.h>
#include <google/protobuf/service.h>
#include <muduo/net/EventLoop.h>
#include <muduo/net/InetAddress.h>
#include <muduo/net/TcpServer.h>

#include <memory>
#include <string>
#include <unordered_map>

// rpc网络服务类
class RpcProvider {
 public:
  void NotifyService(google::protobuf::Service* service);

  // 启动网络调用函数
  void Run(int nodeIndex, short port);

  ~RpcProvider();

 private:
  void OnConnection(const muduo::net::TcpConnectionPtr& conn);

  void OnMessage(const muduo::net::TcpConnectionPtr&, muduo::net::Buffer*,
                 muduo::Timestamp);

  void callmeback(const muduo::net::TcpConnectionPtr& conn,
                  google::protobuf::Message* response);

  struct ServiceInfo {
    google::protobuf::Service* service_ptr;
    std::map<std::string, const google::protobuf::MethodDescriptor*> method_dic;
  };
  std::unordered_map<std::string, ServiceInfo> service_dic;
  muduo::net::EventLoop m_eventLoop;
  std::shared_ptr<muduo::net::TcpServer> m_muduo_server;
};

#endif  // !RPC_PROVIDER_H_
