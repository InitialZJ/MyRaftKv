#include "raft.h"

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>

#include "util.h"

void Raft::AppendEntries1(const raftRpcProctoc::AppendEntriesArgs* args,
                          raftRpcProctoc::AppendEntriesReply* reply) {
  std::lock_guard<std::mutex> locker(m_mtx);
  reply->set_appstate(AppNomal);  // 能接收到说明网络是正常的

  // 1. 若 term < currentTerm，返回 false
  if (args->term() < m_currentTerm) {
    reply->set_success(false);
    reply->set_term(m_currentTerm);
    reply->set_updatenextindex(-100);  // 让Leader及时更新自己
    DPrintf(
        "[func-AppendEntries-rf[%d]]拒绝了, 因为Leader-[%d]的term [%d] < "
        "rf-[%d]的term [%d]",
        m_me, args->leaderid(), args->term(), m_me, m_currentTerm);
    return;  // 从过期的Leader收到消息不需要重设定时器
  }

  Defer ec1([this]() -> void { this->persist(); });
  if (args->term() > m_currentTerm) {
    // 正常情况，更改为Follower
    // DPrintf(
    //     "[func-AppendEntries-rf-[%d]]成为Follower并更新term, "
    //     "因为Leader-[%d]的term [%d] < rf-[%d]的term [%d]",
    //     m_me, args->leaderid(), args->term(), m_me, m_currentTerm);
    m_status = Follower;
    m_currentTerm = args->term();
    m_votedFor = -1;
  }
  myAssert(args->term() == m_currentTerm,
           format("assert {args.Term == rf.currentTerm} fail"));
  // 如果发生网络分区，可能会收到同一个term的Leader的消息，需要转为Follower
  m_status = Follower;
  m_lastResetElectionTime = now();
  // DPrintf("[AppendEntries-func-rf-[%d]]重置了选举超时定时器", m_me);

  // 2. 若日志中不包含index 值和 Term ID 与 prevLogIndex 和 prevLogTerm
  // 相同的记录，返回 false
  // 不能无脑地从prevlogIndex开始记录日志，因为rpc可能会有延迟，导致发过来的log是很久之前的
  // 那么需要比较日志
  if (args->prevlogindex() > getLastLogIndex()) {
    // Leader日志太新
    reply->set_success(false);
    reply->set_term(m_currentTerm);
    reply->set_updatenextindex(getLastLogIndex() + 1);
    return;
  } else if (args->prevlogindex() < m_lastSnapshotIncludeIndex) {
    // Leader日志太老，prevlogindex还没跟上快照
    reply->set_success(false);
    reply->set_term(m_currentTerm);
    reply->set_updatenextindex(m_lastSnapshotIncludeIndex + 1);
  }

  // 3. 如果日志中存在与正在备份的日志记录相冲突的记录（有相同的 index 值但 Term
  // ID 不同），删除该记录以及之后的所有记录
  if (matchLog(args->prevlogindex(), args->prevlogterm())) {
    // 4. 在保存的日志后追加新的日志记录
    for (int i = 0; i < args->entries_size(); i++) {
      auto log = args->entries(i);
      if (log.logindex() > getLastLogIndex()) {
        // 超过就直接添加日志
        m_logs.push_back(log);
      } else {
        // 没超过就比较是否匹配，不匹配再更新，而不是直接截断
        if (m_logs[getSlicesIndexFromLogIndex(log.logindex())].logterm() ==
                log.logterm() &&
            m_logs[getSlicesIndexFromLogIndex(log.logindex())].command() !=
                log.command()) {
          // 相同位置的log，其logTerm相等，但是命令却不相同，不符合raft的前向匹配
          myAssert(
              false,
              format(
                  "[func-AppendEntries-rf-[%d]] 两节点logIndex [%d] 和term "
                  "[%d] 相同, 但command [%d:%d] [%d:%d]却不同\n",
                  m_me, log.logindex(), log.logterm(), m_me,
                  m_logs[getSlicesIndexFromLogIndex(log.logindex())].command(),
                  args->leaderid(), log.command()));
        }

        if (m_logs[getSlicesIndexFromLogIndex(log.logindex())].logterm() !=
            log.logterm()) {
          m_logs[getSlicesIndexFromLogIndex(log.logindex())] = log;
        }
      }
    }

    myAssert(getLastLogIndex() >= args->prevlogindex() + args->entries_size(),
             format("[func-AppendEntries1-rf[%d]] rf.getLastLogIndex() [%d] < "
                    "args.PrevLogIndex[%d] + len(args.Entries) [%d]",
                    m_me, getLastLogIndex(), args->prevlogindex(),
                    args->entries_size()));
    // 5. 若 leaderCommit > commitIndex，令 commitIndex 等于 leaderCommit
    // 和最后一个新日志记录的 index 值之间的最小值
    if (args->leadercommit() > m_commitIndex) {
      m_commitIndex = std::min(args->leadercommit(), getLastLogIndex());
    }

    // 领导会一次发完所有日志
    myAssert(getLastLogIndex() >= m_commitIndex,
             format("[func-AppendEntries1-rf-[%d]] rf.getLastLogIndex [%d] < "
                    "rf.commidIndex [%d]",
                    m_me, getLastLogIndex(), m_commitIndex));
    reply->set_success(true);
    reply->set_term(m_currentTerm);
    return;
  } else {
    // prevLogIndex长度合适，但是不匹配，因此往前寻找矛盾的term的第一个元素
    reply->set_updatenextindex(args->prevlogindex());
    for (int index = args->prevlogindex(); index >= m_lastSnapshotIncludeIndex;
         --index) {
      if (getLogTermFromLogIndex(index) !=
          getLogTermFromLogIndex(args->prevlogindex())) {
        reply->set_updatenextindex(index + 1);
        break;
      }
    }
    reply->set_success(false);
    reply->set_term(m_currentTerm);
    return;
  }
}

// TODO: 这个是干嘛的
void Raft::applierTicker() {
  while (true) {
    m_mtx.lock();
    if (m_status == Leader) {
      DPrintf(
          "[Raft::applierTicker() - raft [%d]] m_lastApplied [%d] "
          "m_commitIndex [%d]",
          m_me, m_lastApplied, m_commitIndex);
    }
    auto applyMsgs = getApplyLogs();
    m_mtx.unlock();

    if (!applyMsgs.empty()) {
      DPrintf(
          "[func-Raft::applierTicker() - raft [%d]] "
          "向kvserver报告的applyMsgs长度为: [%d]",
          m_me, applyMsgs.size());
    }
    for (auto& message : applyMsgs) {
      applyChan->Push(message);
    }
    sleepNMilliseconds(ApplyInterval);
  }
}

// TODO: 这个是干嘛的
bool Raft::CondInstallSnapshot(int lastIncludedTerm, int lastIncludedIndex,
                               std::string snapshot) {
  return true;
}

void Raft::doElection() {
  std::lock_guard<std::mutex> g(m_mtx);

  if (m_status != Leader) {
    DPrintf("[ticker-func-rf-[%d]] 选举定时器到期且不是Leader，开始选举", m_me);
    m_status = Candidate;
    m_currentTerm += 1;
    m_votedFor = m_me;
    persist();
    std::shared_ptr<int> voteNum = std::make_shared<int>(1);
    m_lastResetElectionTime = now();
    for (int i = 0; i < m_peers.size(); i++) {
      if (i == m_me) {
        continue;
      }
      int lastLogIndex = -1, lastLogTerm = -1;
      getLastLogIndexAndTerm(&lastLogIndex, &lastLogTerm);

      std::shared_ptr<raftRpcProctoc::RequestVoteArgs> requestVoteArgs =
          std::make_shared<raftRpcProctoc::RequestVoteArgs>();
      requestVoteArgs->set_term(m_currentTerm);
      requestVoteArgs->set_candidateid(m_me);
      requestVoteArgs->set_lastlogindex(lastLogIndex);
      requestVoteArgs->set_lastlogterm(lastLogTerm);
      std::shared_ptr<raftRpcProctoc::RequestVoteReply> requestVoteReply =
          std::make_shared<raftRpcProctoc::RequestVoteReply>();

      // 使用匿名函数执行避免其拿到锁
      std::thread t(&Raft::sendRequestVote, this, i, requestVoteArgs,
                    requestVoteReply, voteNum);
      t.detach();
    }
  }
}

void Raft::doHeartBeat() {
  std::lock_guard<std::mutex> g(m_mtx);

  if (m_status == Leader) {
    DPrintf("[func-Raft::doHeartBeat()-Leader: [%d]] Leader的心跳定时器触发了",
            m_me);
    auto appendNums = std::make_shared<int>(1);  // 正确返回的节点的数量

    // 对Follower发送
    for (int i = 0; i < m_peers.size(); i++) {
      if (i == m_me) {
        continue;
      }
      DPrintf(
          "[func-Raft::doHeartBeat()-Leader: [%d]] Leader的心跳定时器出发了 "
          "index: [%d]",
          m_me, i);
      myAssert(m_nextIndex[i] >= 1,
               format("rf.nextIndex[%d] = [%d]", i, m_nextIndex[i]));
      // 日志压缩加入之后要判断是发送快照还是AppendEntries
      if (m_nextIndex[i] <= m_lastSnapshotIncludeIndex) {
        std::thread t(&Raft::leaderSendSnapshot, this, i);
        t.detach();
        continue;
      }

      int prevLogIndex = -1, prevLogTerm = -1;
      getPrevLogInfo(i, &prevLogIndex, &prevLogTerm);
      std::shared_ptr<raftRpcProctoc::AppendEntriesArgs> appendEntriesArgs =
          std::make_shared<raftRpcProctoc::AppendEntriesArgs>();
      appendEntriesArgs->set_term(m_currentTerm);
      appendEntriesArgs->set_leaderid(m_me);
      appendEntriesArgs->set_prevlogindex(prevLogIndex);
      appendEntriesArgs->set_prevlogterm(prevLogTerm);
      appendEntriesArgs->clear_entries();
      appendEntriesArgs->set_leadercommit(m_commitIndex);
      if (prevLogIndex != m_lastSnapshotIncludeIndex) {
        for (int j = getSlicesIndexFromLogIndex(prevLogIndex) + 1;
             j < m_logs.size(); ++j) {
          raftRpcProctoc::LogEntry* sendEntryPtr =
              appendEntriesArgs->add_entries();
          *sendEntryPtr = m_logs[j];
        }
      } else {
        for (const auto& item : m_logs) {
          raftRpcProctoc::LogEntry* sendEntryPtr =
              appendEntriesArgs->add_entries();
          *sendEntryPtr = item;
        }
      }

      int lastLogIndex = getLastLogIndex();
      // Leader对每个节点发送的日志长短不一，但是都保证从prevIndex发送直到最后
      myAssert(appendEntriesArgs->prevlogindex() +
                       appendEntriesArgs->entries_size() ==
                   lastLogIndex,
               format("appendEntriesArgs.PrevLogIndex[%d]+len("
                      "appendEntriesArgs.Entries)[%d] != lastLogIndex[%d]",
                      appendEntriesArgs->prevlogindex(),
                      appendEntriesArgs->entries_size(), lastLogIndex));
      // 构造返回值
      const std::shared_ptr<raftRpcProctoc::AppendEntriesReply>
          appendEntriesReply =
              std::make_shared<raftRpcProctoc::AppendEntriesReply>();
      appendEntriesReply->set_appstate(Disconnected);

      std::thread t(&Raft::sendAppendEntries, this, i, appendEntriesArgs,
                    appendEntriesReply, appendNums);
      t.detach();
    }
    m_lastResetHeartBeatTime = now();
  }
}

void Raft::electionTimeOutTicker() {
  while (true) {
    m_mtx.lock();
    auto nowTime = now();
    auto suitableSleepTime =
        getRandomizedElectionTimeout() + m_lastResetElectionTime - now();
    if (suitableSleepTime.count() > 1) {
      std::this_thread::sleep_for(suitableSleepTime);
    }
    if ((m_lastResetElectionTime - nowTime).count() > 0) {
      // 说明睡眠的这段时间有重置定时器，那么就开启新计时
      continue;
    }
    doElection();
  }
}

std::vector<ApplyMsg> Raft::getApplyLogs() {
  std::vector<ApplyMsg> applyMsgs;
  myAssert(
      m_commitIndex <= getLastLogIndex(),
      format(
          "[func-getApplyLogs-rf-[%d]] commitIndex [%d] > getLastLogIndex [%d]",
          m_me, m_commitIndex, getLastLogIndex()));
  while (m_lastApplied < m_commitIndex) {
    m_lastApplied++;
    myAssert(
        m_logs[getSlicesIndexFromLogIndex(m_lastApplied)].logindex() ==
            m_lastApplied,
        format("rf.logs[rf.getSlicesIndexFromLogIndex(rf.lastApplied)]."
               "logindxe [%d] != rf.lastApplied [%d] ",
               m_logs[getSlicesIndexFromLogIndex(m_lastApplied)].logindex(),
               m_lastApplied));
    ApplyMsg applyMsg;
    applyMsg.CommandValid = true;
    applyMsg.SnapshotValid = false;
    applyMsg.Command =
        m_logs[getSlicesIndexFromLogIndex(m_lastApplied)].command();
    applyMsg.CommandIndex = m_lastApplied;
    applyMsgs.emplace_back(applyMsg);
  }
  return applyMsgs;
}

// 新命令应该分配的index
int Raft::getNewCommandIndex() { return getLastLogIndex() + 1; }

void Raft::getPrevLogInfo(int server, int* preIndex, int* preTerm) {
  // prevLogIndex: 在正在备份的日志记录之前的日志记录的 index 值
  // prevLogTerm: 在正在备份的日志记录之前的日志记录的 Term ID
  if (m_nextIndex[server] == m_lastSnapshotIncludeIndex + 1) {
    *preIndex = m_lastSnapshotIncludeIndex;
    *preTerm = m_lastSnapshotIncludeTerm;
    return;
  }
  auto nextIndex = m_nextIndex[server];
  *preIndex = nextIndex - 1;
  *preTerm = m_logs[getSlicesIndexFromLogIndex(*preIndex)].logterm();
}

void Raft::GetState(int* term, bool* isLeader) {
  m_mtx.lock();
  Defer ec1([this]() -> void { m_mtx.unlock(); });
  *term = m_currentTerm;
  *isLeader = (m_status == Leader);
}

void Raft::InstallSnapshot(const raftRpcProctoc::InstallSnapshotRequest* args,
                           raftRpcProctoc::InstallSnapshotResponse* reply) {
  m_mtx.lock();
  Defer ec1([this]() -> void { m_mtx.unlock(); });
  if (args->term() < m_currentTerm) {
    reply->set_term(m_currentTerm);
    return;
  }
  if (args->term() > m_currentTerm) {
    m_currentTerm = args->term();
    m_votedFor = -1;
    m_status = Follower;
    persist();
  }
  m_status = Follower;
  m_lastResetElectionTime = now();
  if (args->lastsnapshotincludeindex() <= m_lastSnapshotIncludeIndex) {
    return;
  }
  // 截断日志，修改commitIndex和lastApplied
  int lastLogIndex = getLastLogIndex();
  if (lastLogIndex > args->lastsnapshotincludeindex()) {
    m_logs.erase(
        m_logs.begin(),
        m_logs.begin() +
            getSlicesIndexFromLogIndex(args->lastsnapshotincludeindex()) + 1);

  } else {
    m_logs.clear();
  }
  m_commitIndex = std::max(m_commitIndex, args->lastsnapshotincludeindex());
  m_lastApplied = std::max(m_lastApplied, args->lastsnapshotincludeindex());
  m_lastSnapshotIncludeIndex = args->lastsnapshotincludeindex();
  m_lastSnapshotIncludeTerm = args->lastsnapshotincludeterm();

  reply->set_term(m_currentTerm);
  ApplyMsg msg;
  msg.SnapshotValid = true;
  msg.Snapshot = args->data();
  msg.SnapshotTerm = args->lastsnapshotincludeterm();
  msg.SnapshotIndex = args->lastsnapshotincludeindex();

  applyChan->Push(msg);
  std::thread t(&Raft::pushMsgToKvServer, this, msg);
  t.detach();
  m_persister->Save(persistData(), args->data());
}

void Raft::pushMsgToKvServer(ApplyMsg msg) { applyChan->Push(msg); }

void Raft::leaderHeartBeatTicker() {
  while (true) {
    auto nowTime = now();
    m_mtx.lock();
    auto suitableSleepTime = std::chrono::milliseconds(HeartBeatTimeout) +
                             m_lastResetHeartBeatTime - nowTime;
    m_mtx.unlock();
    if (suitableSleepTime.count() < 1) {
      suitableSleepTime = std::chrono::milliseconds(1);
    }
    std::this_thread::sleep_for(suitableSleepTime);
    if ((m_lastResetHeartBeatTime - nowTime).count() > 0) {
      continue;
    }
    doHeartBeat();
  }
}

void Raft::leaderSendSnapshot(int server) {
  m_mtx.lock();
  raftRpcProctoc::InstallSnapshotRequest args;
  args.set_leaderid(m_me);
  args.set_term(m_currentTerm);
  args.set_lastsnapshotincludeindex(m_lastSnapshotIncludeIndex);
  args.set_lastsnapshotincludeterm(m_lastSnapshotIncludeTerm);
  args.set_data(m_persister->ReadSnapshot());

  raftRpcProctoc::InstallSnapshotResponse reply;
  m_mtx.unlock();
  bool ok = m_peers[server]->InstallSnapshot(&args, &reply);
  m_mtx.lock();
  Defer ec1([this]() -> void { this->m_mtx.unlock(); });
  if (!ok) {
    return;
  }
  if (m_status != Leader || m_currentTerm != args.term()) {
    // 中间释放过锁，可能状态已经改变了
    return;
  }
  // 无论什么时候都要判断term
  if (reply.term() > m_currentTerm) {
    // 三变
    m_currentTerm = reply.term();
    m_votedFor = -1;
    m_status = Follower;
    persist();
    m_lastResetElectionTime = now();
    return;
  }
  m_matchIndex[server] = args.lastsnapshotincludeindex();
  m_nextIndex[server] = m_matchIndex[server] + 1;
}

void Raft::leaderUpdateCommitIndex() {
  m_commitIndex = m_lastSnapshotIncludeIndex;
  for (int index = getLastLogIndex(); index >= m_lastSnapshotIncludeIndex + 1;
       index--) {
    int sum = 0;
    for (int i = 0; i < m_peers.size(); i++) {
      if (i == m_me) {
        sum += 1;
        continue;
      }
      if (m_matchIndex[i] > index) {
        sum += 1;
      }
    }

    // 只有当前term有新提交的，才会更新commitIndex
    if (sum >= m_peers.size() / 2 + 1 &&
        getLogTermFromLogIndex(index) == m_currentTerm) {
      m_commitIndex = index;
      break;
    }
  }
}

void Raft::getLastLogIndexAndTerm(int* lastLogIndex, int* lastLogTerm) {
  if (m_logs.empty()) {
    *lastLogIndex = m_lastSnapshotIncludeIndex;
    *lastLogTerm = m_lastSnapshotIncludeTerm;
  } else {
    *lastLogIndex = m_logs[m_logs.size() - 1].logindex();
    *lastLogTerm = m_logs[m_logs.size() - 1].logterm();
  }
}

int Raft::getLastLogIndex() {
  int lastLogIndex = -1;
  int _ = -1;
  getLastLogIndexAndTerm(&lastLogIndex, &_);
  return lastLogIndex;
}

bool Raft::matchLog(int logIndex, int logTerm) {
  myAssert(
      logIndex >= m_lastSnapshotIncludeIndex && logIndex <= getLastLogIndex(),
      format("不满足: logIndex [%d] >= lastSnapshotIncludeIndex [%d] && "
             "logIndex [%d] <= getLastLogIndex()",
             logIndex, m_lastSnapshotIncludeIndex, logIndex,
             getLastLogIndex()));
  return logTerm == getLogTermFromLogIndex(logIndex);
}

int Raft::getLogTermFromLogIndex(int logIndex) {
  myAssert(logIndex >= m_lastSnapshotIncludeIndex,
           format("[func-getLogTermFromLogIndex-rf [%d]] index [%d] < "
                  "m_lastSnapshotIncludeIndex [%d]",
                  m_me, logIndex, m_lastSnapshotIncludeIndex));
  int lastLogIndex = getLastLogIndex();

  myAssert(logIndex <= lastLogIndex,
           format("[func-getLogTermFromLogIndex-rf [%d]] logIndex [%d] > "
                  "lastLogIndex [%d]",
                  m_me, logIndex, lastLogIndex));

  if (logIndex == m_lastSnapshotIncludeIndex) {
    return m_lastSnapshotIncludeTerm;
  } else {
    return m_logs[getSlicesIndexFromLogIndex(logIndex)].logterm();
  }
}

void Raft::persist() {
  auto data = persistData();
  m_persister->SaveRaftState(data);
}

void Raft::RequestVote(const raftRpcProctoc::RequestVoteArgs* args,
                       raftRpcProctoc::RequestVoteReply* reply) {
  std::lock_guard<std::mutex> lg(m_mtx);
  Defer ec1([this]() -> void {
    // 应该先持久化，再撤销lock
    this->persist();
  });

  // 对args的term的三种情况分别进行处理，大于小于等于自己的term都是不同的处理
  // reason：出现网络分区，该竞选者已经OutOfDate
  // 1. 若 term < currentTerm，返回 false
  if (args->term() < m_currentTerm) {
    reply->set_term(m_currentTerm);
    reply->set_votestate(Expire);
    reply->set_votegranted(false);
    return;
  }

  if (args->term() > m_currentTerm) {
    m_status = Follower;
    m_currentTerm = args->term();
    m_votedFor = -1;
  }
  myAssert(args->term() == m_currentTerm,
           format("[func-rf[%d]] 前面校验过，这里却不相等", m_me));
  int lastLogIndex = getLastLogIndex();
  if (!UpToDate(args->lastlogindex(), args->lastlogterm())) {
    reply->set_term(m_currentTerm);
    reply->set_votegranted(Voted);
    reply->set_votegranted(false);
    return;
  }

  if (m_votedFor != -1 && m_votedFor != args->candidateid()) {
    // 已经投了其他人
    reply->set_term(m_currentTerm);
    reply->set_votestate(Voted);
    reply->set_votegranted(false);
    return;
  } else {
    // 2. 若 votedFor == null
    // 且给定的日志记录信息可得出对方的日志和自己的相同甚至更新，返回 true
    m_votedFor = args->candidateid();
    m_lastResetElectionTime = now();
    reply->set_term(m_currentTerm);
    reply->set_votestate(Normal);
    reply->set_votegranted(true);
    return;
  }
}

bool Raft::UpToDate(int index, int term) {
  int lastIndex = -1;
  int lastTerm = -1;
  getLastLogIndexAndTerm(&lastIndex, &lastTerm);
  return term > lastTerm || (term == lastTerm && index >= lastIndex);
}

int Raft::GetRaftStateSize() { return m_persister->RaftStateSize(); }

int Raft::getSlicesIndexFromLogIndex(int logIndex) {
  myAssert(logIndex > m_lastSnapshotIncludeIndex,
           format("[func-getSlicesIndexFromLogIndex-rf-[%d]] index [%d] <= "
                  "rf.m_lastSnapshotIncludeIndex",
                  m_me, logIndex, m_lastSnapshotIncludeIndex));
  int lastLogIndex = getLastLogIndex();
  myAssert(logIndex <= lastLogIndex,
           format("[func-getSliceIndexFromLogIndex-rf{%d}]  logIndex{%d} > "
                  "lastLogIndex{%d}",
                  m_me, logIndex, lastLogIndex));
  int SliceIndex = logIndex - m_lastSnapshotIncludeIndex - 1;
  return SliceIndex;
}

bool Raft::sendRequestVote(
    int server, std::shared_ptr<raftRpcProctoc::RequestVoteArgs> args,
    std::shared_ptr<raftRpcProctoc::RequestVoteReply> reply,
    std::shared_ptr<int> votedNum) {
  auto start = now();
  DPrintf("[func-sendRequestVote rf{%d}] 向server{%d} 发送 RequestVote 开始",
          m_me, server);
  bool ok = m_peers[server]->RequestVote(args.get(), reply.get());
  DPrintf(
      "[func-sendRequestVote rf{%d}] 向server{%d} 发送 RequestVote "
      "完毕，耗时:{%d} ms",
      m_me, server, now() - start);
  if (!ok) {
    return ok;
  }

  std::lock_guard<std::mutex> lg(m_mtx);
  if (reply->term() > m_currentTerm) {
    m_status = Follower;
    m_currentTerm = reply->term();
    m_votedFor = -1;
    persist();
    return true;
  } else if (reply->term() < m_currentTerm) {
    // 应该不会出现吧
    return true;
  }
  myAssert(reply->term() == m_currentTerm,
           format("assert {reply.Term==rf.currentTerm} fail"));

  if (!reply->votegranted()) {
    return true;
  }

  *votedNum = *votedNum + 1;
  if (*votedNum >= m_peers.size() / 2 + 1) {
    // 变成Leader
    *votedNum = 0;
    if (m_status == Leader) {
      myAssert(false, format("[func-sendRequestVote-rf{%d}]  term:{%d} "
                             "同一个term当两次领导，error",
                             m_me, m_currentTerm));
    }
    m_status = Leader;
    DPrintf(
        "[func-sendRequestVote rf{%d}] elect success  ,current term:{%d} "
        ",lastLogIndex:{%d}\n",
        m_me, m_currentTerm, getLastLogIndex());
    int lastLogIndex = getLastLogIndex();
    for (int i = 0; i < m_nextIndex.size(); i++) {
      m_nextIndex[i] = lastLogIndex + 1;
      m_matchIndex[i] = 0;
    }
    std::thread t(&Raft::doHeartBeat, this);
    t.detach();
    persist();
  }
  return true;
}

bool Raft::sendAppendEntries(
    int server, std::shared_ptr<raftRpcProctoc::AppendEntriesArgs> args,
    std::shared_ptr<raftRpcProctoc::AppendEntriesReply> reply,
    std::shared_ptr<int> appendNums) {
  DPrintf(
      "[func-Raft::sendAppendEntries-raft{%d}] leader 向节点{%d}发送AE rpc开始 "
      "， args->entries_size():{%d}",
      m_me, server, args->entries_size());
  // ok是网络是否正常的ok，不是是否投票
  bool ok = m_peers[server]->AppendEntries(args.get(), reply.get());

  if (!ok) {
    DPrintf(
        "[func-Raft::sendAppendEntries-raft{%d}] leader 向节点{%d}发送AE "
        "rpc失败",
        m_me, server);
    return ok;
  }
  DPrintf(
      "[func-Raft::sendAppendEntries-raft{%d}] leader 向节点{%d}发送AE rpc成功",
      m_me, server);
  if (reply->appstate() == Disconnected) {
    // TODO: 什么情况下会进入这里
    return ok;
  }
  std::lock_guard<std::mutex> lg1(m_mtx);
  // 对reply进行处理
  // 对于rpc通信，无论什么时候都要检查term
  if (reply->term() > m_currentTerm) {
    m_status = Follower;
    m_currentTerm = reply->term();
    m_votedFor = -1;
    return ok;
  } else if (reply->term() < m_currentTerm) {
    DPrintf(
        "[func -sendAppendEntries  rf{%d}]  "
        "节点：{%d}的term{%d}<rf{%d}的term{%d}\n",
        m_me, server, reply->term(), m_me, m_currentTerm);
    return ok;
  }

  if (m_status != Leader) {
    // TODO: 不是Leader怎么进到这里？
    return ok;
  }

  myAssert(reply->term() == m_currentTerm,
           format("reply.Term{%d} != rf.currentTerm{%d}   ", reply->term(),
                  m_currentTerm));
  if (!reply->success()) {
    // 日志不匹配
    if (reply->updatenextindex() != -100) {
      DPrintf(
          "[func -sendAppendEntries  rf{%d}]  "
          "返回的日志term相等，但是不匹配，回缩nextIndex[%d]：{%d}\n",
          m_me, server, reply->updatenextindex());
      m_nextIndex[server] = reply->updatenextindex();
    }
  } else {
    *appendNums = *appendNums + 1;
    DPrintf("节点{%d}返回true，当前*appendNums{%d}", server, *appendNums);
    m_matchIndex[server] = std::max(
        m_matchIndex[server], args->prevlogindex() + args->entries_size());
    m_nextIndex[server] = m_matchIndex[server] + 1;
    int lastLogIndex = getLastLogIndex();

    myAssert(m_nextIndex[server] <= lastLogIndex + 1,
             format("error msg:rf.nextIndex[%d] > lastLogIndex+1, len(rf.logs) "
                    "= %d   lastLogIndex{%d} = %d",
                    server, m_logs.size(), server, lastLogIndex));
    if (*appendNums >= 1 + m_peers.size() / 2) {
      // 可以commit了
      *appendNums = 0;
      if (args->entries_size() > 0) {
        DPrintf(
            "args->entries(args->entries_size()-1).logterm(){%d}   "
            "m_currentTerm{%d}",
            args->entries(args->entries_size() - 1).logterm(), m_currentTerm);
      }
      if (args->entries_size() > 0 &&
          args->entries(args->entries_size() - 1).logterm() == m_currentTerm) {
        DPrintf(
            "当前term有log成功提交，更新leader的m_commitIndex from{%d} to{%d}",
            m_commitIndex, args->prevlogindex() + args->entries_size());
        m_commitIndex = std::max(m_commitIndex,
                                 args->prevlogindex() + args->entries_size());
      }
      myAssert(m_commitIndex <= lastLogIndex,
               format("[func-sendAppendEntries,rf{%d}] lastLogIndex:%d  "
                      "rf.commitIndex:%d\n",
                      m_me, lastLogIndex, m_commitIndex));
    }
  }
  return ok;
}

void Raft::AppendEntries(google::protobuf::RpcController* controller,
                         const ::raftRpcProctoc::AppendEntriesArgs* request,
                         ::raftRpcProctoc::AppendEntriesReply* response,
                         google::protobuf::Closure* done) {
  AppendEntries1(request, response);
  done->Run();
}

void Raft::InstallSnapshot(
    google::protobuf::RpcController* controller,
    const ::raftRpcProctoc::InstallSnapshotRequest* request,
    ::raftRpcProctoc::InstallSnapshotResponse* response,
    google::protobuf::Closure* done) {
  InstallSnapshot(request, response);
  done->Run();
}

void Raft::RequestVote(google::protobuf::RpcController* controller,
                       const ::raftRpcProctoc::RequestVoteArgs* request,
                       ::raftRpcProctoc::RequestVoteReply* response,
                       google::protobuf::Closure* done) {
  RequestVote(request, response);
  done->Run();
}

void Raft::Start(Op command, int* newLogIndex, int* newLogTerm,
                 bool* isLeader) {
  std::lock_guard<std::mutex> lg1(m_mtx);
  if (m_status != Leader) {
    DPrintf("[func-Start-rf{%d}]  is not leader");
    *newLogIndex = -1;
    *newLogTerm = -1;
    *isLeader = false;
    return;
  }

  raftRpcProctoc::LogEntry newLogEntry;
  newLogEntry.set_command(command.asString());
  newLogEntry.set_logterm(m_currentTerm);
  newLogEntry.set_logindex(getNewCommandIndex());
  m_logs.emplace_back(newLogEntry);

  int lastLogIndex = getLastLogIndex();
  // leader应该不停的向各个Follower发送AE来维护心跳和保持日志同步，目前的做法是新的命令来了不会直接执行，而是等待leader的心跳触发
  DPrintf("[func-Start-rf{%d}]  lastLogIndex:%d,command:%s\n", m_me,
          lastLogIndex, &command);
  persist();
  *newLogIndex = newLogEntry.logindex();
  *newLogTerm = newLogEntry.logterm();
  *isLeader = true;
}

void Raft::init(std::vector<std::shared_ptr<RaftRpcUtil>> peers, int me,
                std::shared_ptr<Persister> persister,
                std::shared_ptr<LockQueue<ApplyMsg>> applyCh) {
  m_peers = peers;
  m_persister = persister;
  m_me = me;
  m_mtx.lock();
  this->applyChan = applyCh;
  m_currentTerm = 0;
  m_status = Follower;
  m_commitIndex = 0;
  m_lastApplied = 0;
  m_logs.clear();
  for (int i = 0; i < m_peers.size(); i++) {
    m_matchIndex.push_back(0);
    m_nextIndex.push_back(0);
  }
  m_votedFor = -1;
  m_lastSnapshotIncludeIndex = 0;
  m_lastSnapshotIncludeTerm = 0;
  m_lastResetElectionTime = now();
  m_lastResetHeartBeatTime = now();

  readPersist(m_persister->ReadRaftState());
  if (m_lastSnapshotIncludeIndex > 0) {
    m_lastApplied = m_lastSnapshotIncludeIndex;
  }

  DPrintf(
      "[Init&ReInit] Sever %d, term %d, lastSnapshotIncludeIndex {%d} , "
      "lastSnapshotIncludeTerm {%d}",
      m_me, m_currentTerm, m_lastSnapshotIncludeIndex,
      m_lastSnapshotIncludeTerm);
  m_mtx.unlock();

  std::thread t(&Raft::leaderHeartBeatTicker, this);
  t.detach();
  std::thread t2(&Raft::electionTimeOutTicker, this);
  t2.detach();
  std::thread t3(&Raft::applierTicker, this);
  t3.detach();
}

std::string Raft::persistData() {
  BoostPersistRaftNode boostPersistRaftNode;
  boostPersistRaftNode.m_currentTerm = m_currentTerm;
  boostPersistRaftNode.m_votedFor = m_votedFor;
  boostPersistRaftNode.m_lastSnapshotIncludeIndex = m_lastSnapshotIncludeIndex;
  boostPersistRaftNode.m_lastSnapshotIncludeTerm = m_lastSnapshotIncludeTerm;
  for (auto& item : m_logs) {
    boostPersistRaftNode.m_logs.push_back(item.SerializeAsString());
  }

  std::stringstream ss;
  boost::archive::text_oarchive oa(ss);
  oa << boostPersistRaftNode;
  return ss.str();
}

void Raft::readPersist(std::string data) {
  if (data.empty()) {
    return;
  }
  std::stringstream iss(data);
  boost::archive::text_iarchive ia(iss);
  BoostPersistRaftNode boostPersistRaftNode;
  ia >> boostPersistRaftNode;

  m_currentTerm = boostPersistRaftNode.m_currentTerm;
  m_votedFor = boostPersistRaftNode.m_votedFor;
  m_lastSnapshotIncludeIndex = boostPersistRaftNode.m_lastSnapshotIncludeIndex;
  m_lastSnapshotIncludeTerm = boostPersistRaftNode.m_lastSnapshotIncludeTerm;
  m_logs.clear();
  for (auto& item : boostPersistRaftNode.m_logs) {
    raftRpcProctoc::LogEntry logEntry;
    logEntry.ParseFromString(item);
    m_logs.emplace_back(logEntry);
  }
}

void Raft::Snapshot(int index, std::string snapshot) {
  std::lock_guard<std::mutex> lg(m_mtx);
  if (m_lastSnapshotIncludeIndex >= index || index > m_commitIndex) {
    DPrintf(
        "[func-Snapshot-rf{%d}] rejects replacing log with snapshotIndex %d as "
        "current snapshotIndex %d is larger or smaller ",
        m_me, index, m_lastSnapshotIncludeIndex);
    return;
  }

  auto lastLogIndex = getLastLogIndex();
  // 制造完此快照后剩余的所有日志
  int newLastSnapshotIncludeIndex = index;
  int newLastSnapshotIncludeTerm =
      m_logs[getSlicesIndexFromLogIndex(index)].logterm();
  std::vector<raftRpcProctoc::LogEntry> trunckedLogs;
  for (int i = index + 1; i <= getLastLogIndex(); i++) {
    trunckedLogs.push_back(m_logs[getSlicesIndexFromLogIndex(i)]);
  }
  m_lastSnapshotIncludeIndex = newLastSnapshotIncludeIndex;
  m_lastSnapshotIncludeTerm = newLastSnapshotIncludeTerm;
  m_logs = trunckedLogs;
  m_commitIndex = std::max(m_commitIndex, index);
  m_lastApplied = std::max(m_lastApplied, index);
  m_persister->Save(persistData(), snapshot);

  DPrintf(
      "[SnapShot]Server %d snapshot snapshot index {%d}, term {%d}, loglen "
      "{%d}",
      m_me, index, m_lastSnapshotIncludeTerm, m_logs.size());
  myAssert(m_logs.size() + m_lastSnapshotIncludeIndex == lastLogIndex,
           format("len(rf.logs){%d} + rf.lastSnapshotIncludeIndex{%d} != "
                  "lastLogjInde{%d}",
                  m_logs.size(), m_lastSnapshotIncludeIndex, lastLogIndex));
}
