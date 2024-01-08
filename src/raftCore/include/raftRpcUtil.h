#ifndef RAFTRPC_H_
#define RAFTRPC_H_

#include "raftRPC.pb.h"

class RaftRpcUtil {
 public:
  bool AppendEntries(raftRpcProctoc::AppendEntriesArgs* args,
                     raftRpcProctoc::AppendEntriesReply* response);
  bool InstallSnapshot(raftRpcProctoc::InstallSnapshotRequest* args,
                       raftRpcProctoc::InstallSnapshotResponse* response);
  bool RequestVote(raftRpcProctoc::RequestVoteArgs* args,
                   raftRpcProctoc::RequestVoteReply* response);

  RaftRpcUtil(std::string ip, short port);

 private:
  raftRpcProctoc::raftRpc_Stub* stub_;
};

#endif  // !RAFTRPC_H_
