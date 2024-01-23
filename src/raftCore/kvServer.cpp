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
  } else {
    // raft已经提交了该command，可以正式开始执行了
    if (raftCommitOp.ClientId == op.ClientId &&
        raftCommitOp.RequestId == op.RequestId) {
      std::string value;
      bool exist = false;
      ExecuteGetOpOnKVDB(op, &value, &exist);
      if (exist) {
        reply->set_err(OK);
        reply->set_value(value);
      } else {
        reply->set_err(ErrNoKey);
        reply->set_value("");
      }
    } else {
      reply->set_err(ErrWrongLeader);
    }
  }
  m_mtx.lock();
  auto tmp = waitApplyCh[raftIndex];
  waitApplyCh.erase(raftIndex);
  delete tmp;
  m_mtx.unlock();
}

void KvServer::GetCommandFromRaft(ApplyMsg message) {
  Op op;
  op.parseFromString(message.Command);
  DPrintf(
      "[KvServer::GetCommandFromRaft-kvserver{%d}] , Got Command --> "
      "Index:{%d} , ClientId {%s}, RequestId {%d}, Opreation {%s}, Key :{%s}, "
      "Value :{%s}",
      m_me, message.CommandIndex, &op.ClientId, op.RequestId, &op.Operation,
      &op.Key, &op.Value);
  if (message.CommandIndex <= m_lastSnapShotRaftLogIndex) {
    return;
  }

  if (!ifRequestDuplicate(op.ClientId, op.RequestId)) {
    if (op.Operation == "Put") {
      ExecutePutOpOnKVDB(op);
    }
    if (op.Operation == "Append") {
      ExecuteAppendOpOnKVDB(op);
    }
  }
  // 到这里kvDB已经制作了快照
  if (m_maxRaftState != -1) {
    IfNeedToSendSnapShotCommand(message.CommandIndex, 9);
  }
  SendMessageToWaitChan(op, message.CommandIndex);
}

bool KvServer::ifRequestDuplicate(std::string ClientId, int RequestId) {
  std::lock_guard<std::mutex> lg(m_mtx);
  if (m_lastRequestId.find(ClientId) == m_lastRequestId.end()) {
    return false;
  }
  return RequestId <= m_lastRequestId[ClientId];
}

void KvServer::PutAppend(const raftKVRpcProctoc::PutAppendArgs* args,
                         raftKVRpcProctoc::PutAppendReply* reply) {
  Op op;
  op.Operation = args->op();
  op.Key = args->key();
  op.Value = args->value();
  op.ClientId = args->clientid();
  op.RequestId = args->requestid();
  int raftIndex = -1;
  int _ = -1;
  bool isLeader = false;

  m_raftNode->Start(op, &raftIndex, &_, &isLeader);
  if (!isLeader) {
    DPrintf(
        "[func -KvServer::PutAppend -kvserver{%d}]From Client %s (Request %d) "
        "To Server %d, key %s, raftIndex %d , but not leader",
        m_me, &args->clientid(), args->requestid(), m_me, &op.Key, raftIndex);

    reply->set_err(ErrWrongLeader);
    return;
  }
  DPrintf(
      "[func -KvServer::PutAppend -kvserver{%d}]From Client %s (Request %d) To "
      "Server %d, key %s, raftIndex %d , is leader ",
      m_me, &args->clientid(), args->requestid(), m_me, &op.Key, raftIndex);
  m_mtx.lock();
  if (waitApplyCh.find(raftIndex) == waitApplyCh.end()) {
    waitApplyCh.insert(std::make_pair(raftIndex, new LockQueue<Op>()));
  }
  auto chForRaftIndex = waitApplyCh[raftIndex];
  m_mtx.unlock();

  Op raftCommitOp;
  if (!chForRaftIndex->timeOutPop(CONSENSUS_TIMEOUT, &raftCommitOp)) {
    DPrintf(
        "[func -KvServer::PutAppend -kvserver{%d}]TIMEOUT PUTAPPEND !!!! "
        "Server %d , get Command <-- Index:%d , ClientId %s, RequestId %s, "
        "Opreation %s Key :%s, Value :%s",
        m_me, m_me, raftIndex, &op.ClientId, op.RequestId, &op.Operation,
        &op.Key, &op.Value);
    // 判断RequestId是否过时
    if (ifRequestDuplicate(op.ClientId, op.RequestId)) {
      reply->set_err(OK);
    } else {
      reply->set_err(ErrWrongLeader);
    }
  } else {
    DPrintf(
        "[func -KvServer::PutAppend "
        "-kvserver{%d}]WaitChanGetRaftApplyMessage<--Server %d , get Command "
        "<-- Index:%d , ClientId %s, RequestId %d, Opreation %s, Key :%s, "
        "Value :%s",
        m_me, m_me, raftIndex, &op.ClientId, op.RequestId, &op.Operation,
        &op.Key, &op.Value);
    if (raftCommitOp.ClientId == op.ClientId &&
        raftCommitOp.RequestId == op.RequestId) {
      // 可能发生leader的变更导致日志被覆盖，因此需要检查
      reply->set_err(OK);
    } else {
      reply->set_err(ErrWrongLeader);
    }
  }

  m_mtx.lock();
  auto tmp = waitApplyCh[raftIndex];
  waitApplyCh.erase(raftIndex);
  delete tmp;
  m_mtx.unlock();
}

// 从raft中将command放入waitApplyCh
void KvServer::ReadRaftApplyCommandLoop() {
  while (true) {
    // 如果只操作applyChan不用拿锁，因为applyChan自己带锁
    auto message = applyChan->Pop();
    DPrintf(
        "---------------tmp-------------[func-KvServer::"
        "ReadRaftApplyCommandLoop()-kvserver{%d}] 收到了下raft的消息",
        m_me);
    if (message.CommandValid) {
      GetCommandFromRaft(message);
    }
    if (message.SnapshotValid) {
      GetSnapShotFromRaft(message);
    }
  }
}

// raft会与persist层进行交互，kvserver层也会，因为kvserver层开始的时候需要恢复kvdb的状态
void KvServer::ReadSnapShotToInstall(std::string snapshot) {
  if (snapshot.empty()) {
    return;
  }

  parseFromString(snapshot);
}

bool KvServer::SendMessageToWaitChan(const Op& op, int raftIndex) {
  std::lock_guard<std::mutex> lg(m_mtx);
  DPrintf(
      "[RaftApplyMessageSendToWaitChan--> raftserver{%d}] , Send Command --> "
      "Index:{%d} , ClientId {%d}, RequestId {%d}, Opreation {%v}, Key :{%v}, "
      "Value :{%v}",
      m_me, raftIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key,
      &op.Value);

  if (waitApplyCh.find(raftIndex) == waitApplyCh.end()) {
    return false;
  }
  waitApplyCh[raftIndex]->Push(op);
  DPrintf(
      "[RaftApplyMessageSendToWaitChan--> raftserver{%d}] , Send Command --> "
      "Index:{%d} , ClientId {%d}, RequestId {%d}, Opreation {%v}, Key :{%v}, "
      "Value :{%v}",
      m_me, raftIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key,
      &op.Value);
  return true;
}

void KvServer::IfNeedToSendSnapShotCommand(int raftIndex, int proportion) {
  if (m_raftNode->GetRaftStateSize() > m_maxRaftState / 10.0) {
    auto snapshot = MakeSnapShot();
    m_raftNode->Snapshot(raftIndex, snapshot);
  }
}

void KvServer::GetSnapShotFromRaft(ApplyMsg message) {
  std::lock_guard<std::mutex> lg(m_mtx);
  if (m_raftNode->CondInstallSnapshot(
          message.SnapshotTerm, message.SnapshotIndex, message.Snapshot)) {
    ReadSnapShotToInstall(message.Snapshot);
    m_lastSnapShotRaftLogIndex = message.SnapshotIndex;
  }
}

std::string KvServer::MakeSnapShot() {
  std::lock_guard<std::mutex> lg(m_mtx);
  std::string snapshotData = getSnapshotData();
  return snapshotData;
}

void KvServer::PutAppend(google::protobuf::RpcController* controller,
                         const ::raftKVRpcProctoc::PutAppendArgs* request,
                         ::raftKVRpcProctoc::PutAppendReply* response,
                         google::protobuf::Closure* done) {
  PutAppend(request, response);
  done->Run();
}

void KvServer::Get(google::protobuf::RpcController* controller,
                   const ::raftKVRpcProctoc::GetArgs* request,
                   ::raftKVRpcProctoc::GetReply* response,
                   google::protobuf::Closure* done) {
  Get(request, response);
  done->Run();
}

KvServer::KvServer(int me, int maxraftstate, std::string nodeInforFilename,
                   short port)
    : m_skipList(6) {
  std::shared_ptr<Persister> persister = std::make_shared<Persister>(me);
  m_me = me;
  m_maxRaftState = maxraftstate;
  applyChan = std::make_shared<LockQueue<ApplyMsg>>();
  m_raftNode = std::make_shared<Raft>();

  std::thread t([this, port]() -> void {
    RpcProvider provider;
    provider.NotifyService(this);
    provider.NotifyService(this->m_raftNode.get());
    provider.Run(m_me, port);
  });
  t.detach();

  // 开启rpc远程调用能力，需要注意必须要保证所有节点都开启rpc接受功能之后才能开启rpc远程调用能力
  // 这里使用睡眠来保证
  std::cout << "raftServer node:" << m_me
            << " start to sleep to wait all ohter raftnode start!!!!"
            << std::endl;
  sleep(6);
  std::cout << "raftServer node:" << m_me
            << " wake up!!!! start to connect other raftnode" << std::endl;

  // 获取所有raft节点的ip、port，并进行连接，要排除自己
  MrpcConfig config;
  config.LoadConfigFile(nodeInforFilename.c_str());
  std::vector<std::pair<std::string, short>> ipPortVt;
  for (int i = 0; i < INT_MAX - 1; i++) {
    std::string node = "node" + std::to_string(i);
    std::string nodeIp = config.Load(node + "ip");
    std::string nodePortstr = config.Load(node + "port");
    if (nodeIp.empty()) {
      break;
    }
    ipPortVt.emplace_back(nodeIp, atoi(nodePortstr.c_str()));
  }

  std::vector<std::shared_ptr<RaftRpcUtil>> servers;
  for (int i = 0; i < ipPortVt.size(); i++) {
    if (i == m_me) {
      servers.push_back(nullptr);
      continue;
    }
    std::string otherNodeIp = ipPortVt[i].first;
    short otherNodePort = ipPortVt[i].second;
    auto* rpc = new RaftRpcUtil(otherNodeIp, otherNodePort);
    servers.push_back(std::shared_ptr<RaftRpcUtil>(rpc));
    std::cout << "node: " << m_me << "连接node" << i << "success!" << std::endl;
  }
  sleep(ipPortVt.size() - m_me);
  // kv的server直接与raft通信，但kv不直接与raft通信，所以需要把applyChan传递下去用于通信，两者的persister也是共用的
  m_raftNode->init(servers, m_me, persister, applyChan);

  m_lastSnapShotRaftLogIndex = 0;
  auto snapshot = persister->ReadSnapshot();
  if (!snapshot.empty()) {
    ReadSnapShotToInstall(snapshot);
  }
  std::thread t2(&KvServer::ReadRaftApplyCommandLoop, this);
  t2.join();
}
