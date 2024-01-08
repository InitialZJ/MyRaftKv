#ifndef RAFT_H_
#define RAFT_H_

#include <boost/any.hpp>
#include <boost/serialization/serialization.hpp>
#include <boost/serialization/string.hpp>
#include <boost/serialization/vector.hpp>
#include <chrono>
#include <cmath>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "ApplyMsg.h"
#include "Persister.h"
#include "config.h"
#include "raftRpcUtil.h"
#include "util.h"

// 网络状态表示
constexpr int Disconnected = 0;
constexpr int AppNomal = 1;

// 投票状态
constexpr int Killed = 0;
constexpr int Voted = 1;   // 本轮已经投过票了
constexpr int Expire = 2;  // 投票（消息、竞选者）过期
constexpr int Normal = 3;

class Raft : public raftRpcProctoc::raftRpc {
 public:
  void AppendEntries1(const raftRpcProctoc::AppendEntriesArgs* args,
                      raftRpcProctoc::AppendEntriesReply* reply);
  void applierTicker();
  bool CondInstallSnapshot(int lastIncludedTerm, int lastIncludedIndex,
                           std::string snapshot);
  void doElection();
  void doHeartBeat();
  // 每隔一段时间检查睡眠时间内有没有重置定时器，没有则说明超时了
  // 如果有则设置合适的睡眠时间：睡眠到重置时间 + 超时时间
  void electionTimeOutTicker();
  std::vector<ApplyMsg> getApplyLogs();
  int getNewCommandIndex();
  void getPrevLogInfo(int server, int* prevIndex, int* preTerm);
  int GetState(int* term, bool* isLeader);
  void InstallSnapshot(const raftRpcProctoc::InstallSnapshotRequest* args,
                       raftRpcProctoc::InstallSnapshotResponse* reply);
  void leaderHeartBeatTicker();
  void leaderSendSnapshot(int server);
  void leaderUpdateCommitIndex();
  bool matchLog(int logIndex, int logTerm);
  void persist();
  void RequestVote(const raftRpcProctoc::RequestVoteArgs* args,
                   raftRpcProctoc::RequestVoteReply* reply);
  bool UpToDate(int index, int term);
  int getLastLogIndex();
  void getLastLogIndexAndTerm(int* lastLogIndex, int* lastLogTerm);
  int getLogTermFromLogIndex(int logIndex);
  int GetRaftStateSize();
  int getSlicesIndexFromLogIndex(int logIndex);

  bool sendRequestVote(int server,
                       std::shared_ptr<raftRpcProctoc::RequestVoteArgs> args,
                       std::shared_ptr<raftRpcProctoc::RequestVoteReply> reply,
                       std::shared_ptr<int> votedNum);
  bool sendAppendEntries(
      int server, std::shared_ptr<raftRpcProctoc::AppendEntriesArgs> args,
      std::shared_ptr<raftRpcProctoc::AppendEntriesReply> reply,
      std::shared_ptr<int> appendNums);

  void pushMsgToKvServer(ApplyMsg msg);
  void readPersist(std::string data);
  std::string persistData();

  void Start(Op command, int* newLogIndex, int* newLogTerm, bool* isLeader);
  void Snapshot(int index, std::string snapshot);

  // 重写基类方法，因为RPC远程调用真正调用的是这个方法
  // 序列化、反序列化等操作RPC框架都已经做完了，因此这里只需要获取值然后真正调用本地方法即可
  void AppendEntries(google::protobuf::RpcController* controller,
                     const ::raftRpcProctoc::AppendEntriesArgs* request,
                     ::raftRpcProctoc::AppendEntriesReply* response,
                     google::protobuf::Closure* done) override;
  void InstallSnapshot(google::protobuf::RpcController* controller,
                       const ::raftRpcProctoc::InstallSnapshotRequest* request,
                       ::raftRpcProctoc::InstallSnapshotResponse* response,
                       google::protobuf::Closure* done) override;
  void RequestVote(google::protobuf::RpcController* controller,
                   const ::raftRpcProctoc::RequestVoteArgs* request,
                   ::raftRpcProctoc::RequestVoteReply* response,
                   google::protobuf::Closure* done) override;

  void init(std::vector<std::shared_ptr<RaftRpcUtil>> peers, int me,
            std::shared_ptr<Persister> persister,
            std::shared_ptr<LockQueue<ApplyMsg>> applyCh);

 private:
  std::mutex m_mtx;
  std::vector<std::shared_ptr<RaftRpcUtil>> m_peers;
  std::shared_ptr<Persister> m_persister;
  int m_me;
  int m_currentTerm;
  int m_votedFor;
  std::vector<raftRpcProctoc::LogEntry>
      m_logs;  // 日志条目数组，包含了状态机要执行的指令集，Leader任期号，日志编号
  int m_commitIndex;
  int m_lastApplied;  // 已经汇报给状态机（上层应用）的log的index

  // 以下两个vector下标从1开始
  std::vector<int> m_nextIndex;
  std::vector<int> m_matchIndex;
  enum Status { Follower, Candidate, Leader };

  // 身份
  Status m_status;

  // client从这里取日志
  std::shared_ptr<LockQueue<ApplyMsg>> applyChan;

  // 选举超时
  std::chrono::_V2::system_clock::time_point m_lastResetElectionTime;
  // 心跳超时
  std::chrono::_V2::system_clock::time_point m_lastResetHeartBeatTime;

  // 快照中最后一个日志的Index和Term
  int m_lastSnapshotIncludeIndex;
  int m_lastSnapshotIncludeTerm;

  // for persist
  class BoostPersistRaftNode {
   public:
    friend class boost::serialization::access;
    template <class Archive>
    void serialize(Archive& ar, const unsigned int version) {
      ar & m_currentTerm;
      ar & m_votedFor;
      ar & m_lastSnapshotIncludeIndex;
      ar & m_lastSnapshotIncludeTerm;
      ar & m_logs;
    }
    int m_currentTerm;
    int m_votedFor;
    int m_lastSnapshotIncludeIndex;
    int m_lastSnapshotIncludeTerm;
    std::vector<std::string> m_logs;
    std::unordered_map<std::string, int> umap;
  };
};

#endif  // !RAFT_H_
