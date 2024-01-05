#include "RpcProvider.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>

#include <cstring>
#include <fstream>
#include <functional>
#include <iostream>
#include <string>

#include "rpc_header.pb.h"
#include "util.h"

void RpcProvider::NotifyService(google::protobuf::Service* service) {
  const google::protobuf::ServiceDescriptor* service_ptr =
      service->GetDescriptor();
  const std::string service_name = service_ptr->name();
  // 得到method的数量
  int n = service_ptr->method_count();
  std::map<std::string, const google::protobuf::MethodDescriptor*> method_dic;
  // 遍历方法，进行存储
  ServiceInfo sinfo;
  for (int i = 0; i < n; i++) {
    const google::protobuf::MethodDescriptor* method_ptr =
        service_ptr->method(i);
    const std::string method_name = method_ptr->name();
    sinfo.method_dic.insert({method_name, method_ptr});
  }
  sinfo.service_ptr = service;
  service_dic.insert({service_name, sinfo});
}

// 启动rpc服务节点，开始提供rpc远程网络服务调用
void RpcProvider::Run(int nodeIndex, short port) {
  char* ipC;
  char hname[128];
  struct hostent* hent;
  gethostname(hname, sizeof(hname));
  hent = gethostbyname(hname);
  for (int i = 0; hent->h_addr_list[i]; i++) {
    ipC = inet_ntoa(*(struct in_addr*)(hent->h_addr_list[i]));
  }
  std::string ip = std::string(ipC);

  std::string node = "node" + std::to_string(nodeIndex);
  std::ofstream outfile;
  outfile.open("test.conf", std::ios::app);  // 以追加模式写入文件
  if (!outfile.is_open()) {
    std::cout << "打开文件失败！" << std::endl;
    exit(EXIT_FAILURE);
  }
  outfile << node << "ip=" << ip << std::endl;
  outfile << node << "port=" << std::to_string(port) << std::endl;
  outfile.close();

  muduo::net::InetAddress address(ip, port);
  m_muduo_server = std::make_shared<muduo::net::TcpServer>(
      &m_eventLoop, address, "RpcProvider");
  // 绑定连接回调和消息读写回调方法，分离网络代码和业务代码
  m_muduo_server->setConnectionCallback(
      std::bind(&RpcProvider::OnConnection, this, std::placeholders::_1));
  m_muduo_server->setMessageCallback(
      std::bind(&RpcProvider::OnMessage, this, std::placeholders::_1,
                std::placeholders::_2, std::placeholders::_3));
  m_muduo_server->setThreadNum(4);
  std::cout << "RpcProvider start server at ip: " << ip << " port: " << port
            << std::endl;

  // 启动网络服务
  m_muduo_server->start();
  m_eventLoop.loop();
}

void RpcProvider::OnConnection(const muduo::net::TcpConnectionPtr& conn) {
  // 如果是新连接就什么都不干，即正常的接收连接即可
  if (!conn->connected()) {
    conn->shutdown();
  }
}

// 当有字节流时，会自动调用该函数
// 当函数解析字符流时，调用相应注册的函数
void RpcProvider::OnMessage(const muduo::net::TcpConnectionPtr& conn,
                            muduo::net::Buffer* buffer,
                            muduo::Timestamp timestamp) {
  std::string buffer_str = buffer->retrieveAllAsString();
  // 截取前四个字节
  uint32_t len = 0;
  buffer_str.copy((char*)&len, 4, 0);
  // 解析rpc_header对象
  std::string rpc_header_str = buffer_str.substr(4, len);
  src::RpcHeader rpcheader = src::RpcHeader();
  if (!rpcheader.ParseFromString(rpc_header_str)) {
    std::cout << "rpcheader解析失败" << std::endl;
    return;
  }

  std::string service_name = rpcheader.service_name();
  std::string method_name = rpcheader.method_name();
  uint32_t args_len = rpcheader.args_len();

  // 找到对应的service method
  if (!service_dic.count(service_name)) {
    std::cout << "服务名不存在" << std::endl;
    return;
  }

  ServiceInfo service_info = service_dic[service_name];
  google::protobuf::Service* service = service_info.service_ptr;
  if (!service_info.method_dic.count(method_name)) {
    std::cout << "方法名不存在" << std::endl;
    return;
  }

  const google::protobuf::MethodDescriptor* method_des =
      service_info.method_dic[method_name];
  // 截取args，此时的args为request
  std::string args_str = buffer_str.substr(4 + len, args_len);
  // 解析args，需要获得service method下的request
  // 直接使用基类message即可，message是一个抽象类，不能实例化
  // 因此还需要找method的方法，创建一个新对象
  google::protobuf::Message* request =
      service->GetRequestPrototype(method_des).New();
  google::protobuf::Message* response =
      service->GetResponsePrototype(method_des).New();
  if (!request->ParseFromString(args_str)) {
    std::cout << "解析参数失败" << std::endl;
    return;
  }

  // 生成回调函数
  auto done = google::protobuf::NewCallback<RpcProvider,
                                            const muduo::net::TcpConnectionPtr&,
                                            google::protobuf::Message*>(
      this, &RpcProvider::callmeback, conn, response);

  // 调用函数
  service->CallMethod(method_des, nullptr, request, response, done);
}

void RpcProvider::callmeback(const muduo::net::TcpConnectionPtr& conn,
                             google::protobuf::Message* response) {
  // 序列化
  std::string response_str;
  if (!response->SerializeToString(&response_str)) {
    std::cout << "response序列化失败" << std::endl;
    return;
  }
  conn->send(response_str);
}

RpcProvider::~RpcProvider() {
  std::cout << "[func - RpcProvider::~RpcProvider()]: ip和port信息: "
            << m_muduo_server->ipPort() << std::endl;
  m_eventLoop.quit();
}
