#include "kvServer.h"

#include "MrpcConfig.h"
#include "RpcProvider.h"

void KvServer::DprintfKVDB() {
  if (!Debug) {
    return;
  }
  std::lock_guard<std::mutex> lg(m_mtx);
  Defer ec1([this]() -> void { m_skipList.display_list(); });
}

void KvServer::ExecuteAppendOpOnKVDB(Op op) {
  m_mtx.lock();
  m_skipList.insert_set_element(op.Key, op.Value);
  m_lastRequestId[op.ClientId] = op.RequestId;
  m_mtx.unlock();
  DprintfKVDB();
}

void KvServer::ExecuteGetOpOnKVDB(Op op, std::string* value, bool* exist) {
  m_mtx.lock();
  *value = "";
  *exist = false;
  if (m_skipList.search_element(op.Key, *value)) {
    *exist = true;
  }
  m_lastRequestId[op.ClientId] = op.RequestId;
  m_mtx.unlock();
  DprintfKVDB();
}

void KvServer::ExecutePutOpOnKVDB(Op op) {
  m_mtx.lock();
  m_skipList.insert_set_element(op.Key, op.Value);
  m_lastRequestId[op.ClientId] = op.RequestId;
  m_mtx.unlock();
  DprintfKVDB();
}

void KvServer::Get(const raftKVRpcProctoc::GetArgs* args,
                   raftKVRpcProctoc::GetReply* reply) {
  Op op;
  op.Operation = "Get";
  op.Key = args->key();
  op.Value = "";
  op.ClientId = args->clientid();
  op.RequestId = args->requestid();

  int raftIndex = -1;
  int _ = -1;
  bool isLeader = false;
  m_raftNode->Start(op, &raftIndex, &_, &isLeader);

  if (!isLeader) {
    reply->set_err(ErrWrongLeader);
    return;
  }

  m_mtx.lock();
  if (waitApplyCh.find(raftIndex) == waitApplyCh.end()) {
    waitApplyCh.insert(std::make_pair(raftIndex, new LockQueue<Op>()));
  }
  auto chForRaftIndex = waitApplyCh[raftIndex];
  m_mtx.unlock();

  Op raftCommitOp;
  if (!chForRaftIndex->timeOutPop(CONSENSUS_TIMEOUT, &raftCommitOp)) {
    int _ = -1;
    bool isLeader = false;
    m_raftNode->GetState(&_, &isLeader);

    if (ifRequestDuplicate(op.ClientId, op.RequestId) && isLeader) {
      // 如果超时，代表raft集群不保证已经commmit该日志，但是如果是已经提交过的get请求，是可以再执行的，不会违反线性一致性
			std::string value;
			bool exist = false;
			// TODO: 这个执行怎么跟上面的不一样
			ExecuteGetOpOnKVDB(op, &value, &exist);
			if (exist) {
				reply->set_err(OK);
				reply->set_value(value);
			} else {
				reply->set_err(ErrNoKey);
				reply->set_value("");
			}
    } else {
			// 让clerk换一个节点重试
			reply->set_err(ErrWrongLeader);
		}
  }
}
