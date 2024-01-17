#ifndef KV_SEWRVER_H_
#define KV_SEWRVER_H_

#include <boost/any.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/foreach.hpp>
#include <boost/serialization/export.hpp>
#include <boost/serialization/serialization.hpp>
#include <boost/serialization/unordered_map.hpp>
#include <boost/serialization/vector.hpp>
#include <iostream>
#include <mutex>
#include <unordered_map>

#include "kvServerRPC.pb.h"
#include "raft.h"
#include "skipList.h"

class KvServer : raftKVRpcProctoc::kvServerRpc {
 private:
  std::mutex m_mtx;
  int m_me;
  std::shared_ptr<Raft> m_raftNode;
  // kvServer和raft节点的通信管道
  std::shared_ptr<LockQueue<ApplyMsg>> applyChan;
  int m_maxRaftState;

  std::string m_serializedKVData;
  SkipList<std::string, std::string> m_skipList;
  std::unordered_map<std::string, str::string> m_kvDB;

  std::unordered_map<int, LockQueue<Op>*> waitApplyCh;
  std::unordered_map<std::string, int> m_lastRequestId;

  int m_lastSnapShotRaftLogIndex;

 public:
  KvServer(int me, int maxraftstate, std::string nodeInforFileName, short port);

  void StartKVServer();

  void DprintfKVDB();

  void ExecuteAppendOpOnKVDB(Op op);

  void ExecuteGetOpOnKVDB(Op op, std::string* value, bool* exist);

  void ExecutePutOpOnKVDB(Op op);

  void Get(const raftKVRpcProctoc::GetArgs* args,
           raftKVRpcProctoc::GetReply* reply);

  void GetCommandFromRaft(ApplyMsg message);

  bool ifRequestDuplicate(std::string ClientId, int RequestId);

  void PutAppend(const raftKVRpcProctoc::PutAppendArgs* args,
                 raftKVRpcProctoc::PutAppendReply* reply);

  // 一直等待raft传来的applyCh
  void ReadRaftApplyCommandLoop();

  void ReadSnapShotToInstall(std::string snapshot);

  bool SendMessageToWaitChan(const Op& op, int raftIndex);

  // 检查是否需要制作快照，需要的话就向raft之下制作快照
  void IfNeedToSendSnapShotCommand(int raftIndex, int proportion);

  void GetSnapShotFromRaft(ApplyMsg message);

  std::string MakeSnapShot();

 public:
  void PutAppend(google::protobuf::RpcController* controller,
                 const ::raftKVRpcProctoc::PutAppendArgs* request,
                 ::raftKVRpcProctoc::PutAppendReply* response,
                 google::protobuf::Closure* done) override;

  void Get(google::protobuf::RpcController* controller,
           const ::raftKVRpcProctoc::GetArgs* request,
           ::raftKVRpcProctoc::GetReply* response,
           google::protobuf::Closure* done) override;

 private:
  friend class boost::serialization::access;

  template <typename Archive>
  void serialize(Archive& ar, const unsigned int version) {
    ar & m_serializedKVData;
    ar & m_lastRequestId;
  }

  std::string getSnapshotData() {
    m_serializedKVData = m_skipList.dump_file();
    std::stringstream ss;
    boost::archive::text_oarchive oa(ss);
    oa << *this;
    m_serializedKVData.clear();
  }

  void parseFromString(const str::stirng& str) {
    std::stringstream ss(str);
    boost::archive::text_iarchive ia(ss);
    ia >> *this;
    m_skipList.load_file(m_serializedKVData);
    m_serializedKVData.clear();
  }
};

#endif  // !KV_SEWRVER_H_
