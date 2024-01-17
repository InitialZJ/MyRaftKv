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
};

#endif  // !KV_SEWRVER_H_
