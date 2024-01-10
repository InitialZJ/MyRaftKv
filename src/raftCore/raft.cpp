#include "raft.h"

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>

#include "util.h"

void Raft::AppendEntries1(const raftRpcProctoc::AppendEntriesArgs* args,
                          raftRpcProctoc::AppendEntriesReply* reply) {
  std::lock_guard<std::mutex> locker(m_mtx);
  reply->set_appstate(AppNomal);  // 能接收到说明网络是正常的

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

  if (matchLog(args->prevlogindex(), args->prevlogterm())) {
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
