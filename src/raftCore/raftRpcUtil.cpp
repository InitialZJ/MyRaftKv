#include "raftRpcUtil.h"

#include "MrpcChannel.h"
#include "MrpcController.h"

bool RaftRpcUtil::AppendEntries(raftRpcProctoc::AppendEntriesArgs* args,
                                raftRpcProctoc::AppendEntriesReply* response) {
  MrpcController controller;
  stub_->AppendEntries(&controller, args, response, nullptr);
  return !controller.Failed();
}

bool RaftRpcUtil::InstallSnapshot(
    raftRpcProctoc::InstallSnapshotRequest* args,
    raftRpcProctoc::InstallSnapshotResponse* response) {
  MrpcController controller;
  stub_->InstallSnapshot(&controller, args, response, nullptr);
  return !controller.Failed();
}

bool RaftRpcUtil::RequestVote(raftRpcProctoc::RequestVoteArgs* args,
                              raftRpcProctoc::RequestVoteReply* response) {
  MrpcController controller;
  stub_->RequestVote(&controller, args, response, nullptr);
  return !controller.Failed();
}

RaftRpcUtil::RaftRpcUtil(std::string ip, short port) {
  stub_ = new raftRpcProctoc::raftRpc_Stub(new MrpcChannel(ip, port, true));
}

RaftRpcUtil::~RaftRpcUtil() { delete stub_; }
