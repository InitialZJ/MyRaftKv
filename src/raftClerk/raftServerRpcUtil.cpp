#include "raftServerRpcUtil.h"

raftServerRpcUtil::raftServerRpcUtil(std::string ip, short port) {
  stub =
      new raftKVRpcProctoc::kvServerRpc_Stub(new MrpcChannel(ip, port, false));
}

raftServerRpcUtil::~raftServerRpcUtil() { delete stub; }

bool raftServerRpcUtil::Get(raftKVRpcProctoc::GetArgs* args,
                            raftKVRpcProctoc::GetReply* reply) {
  MrpcController controller;
  stub->Get(&controller, args, reply, nullptr);
  return !controller.Failed();
}

bool raftServerRpcUtil::PutAppend(raftKVRpcProctoc::PutAppendArgs* args,
                                  raftKVRpcProctoc::PutAppendReply* reply) {
  MrpcController controller;
  stub->PutAppend(&controller, args, reply, nullptr);
  if (controller.Failed()) {
    std::cout << controller.ErrorText() << std::endl;
  }
  return !controller.Failed();
}
