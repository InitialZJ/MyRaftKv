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

	
}
