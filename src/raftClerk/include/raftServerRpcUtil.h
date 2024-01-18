#ifndef RAFT_SERVER_RPC_UTIL_H_
#define RAFT_SERVER_RPC_UTIL_H_

#include <iostream>

#include "MrpcChannel.h"
#include "MrpcController.h"
#include "RpcProvider.h"
#include "kvServerRPC.pb.h"

class raftServerRpcUtil {
 private:
  raftKVRpcProctoc::kvServerRpc_Stub* stub;

 public:
  bool Get(raftKVRpcProctoc::GetArgs* GetArgs,
           raftKVRpcProctoc::GetReply* reply);
  bool PutAppend(raftKVRpcProctoc::PutAppendArgs* args,
                 raftKVRpcProctoc::PutAppendReply* reply);

  raftServerRpcUtil(std::string ip, short port);
  ~raftServerRpcUtil();
};

#endif  // !RAFT_SERVER_RPC_UTIL_H_
