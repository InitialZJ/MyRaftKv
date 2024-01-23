#include "Persister.h"

#include "util.h"

Persister::Persister(int me)
    : raftStateFile("raftStatePersist" + std::to_string(me) + ".txt"),
      snapshotFile("snapshotPersist" + std::to_string(me) + ".txt"),
      m_raftStateSize(0) {
  std::fstream file(raftStateFile, std::ios::out | std::ios::trunc);
  if (file.is_open()) {
    file.close();
  }
  file = std::fstream(snapshotFile, std::ios::out | std::ios::trunc);
  if (file.is_open()) {
    file.close();
  }

  m_raftStateOutStream.open(raftStateFile);
  m_snapshotOutStream.open(snapshotFile);
}

Persister::~Persister() {
  if (m_raftStateOutStream.is_open()) {
    m_raftStateOutStream.close();
  }
  if (m_snapshotOutStream.is_open()) {
    m_snapshotOutStream.close();
  }
}

void Persister::Save(const std::string raftState, const std::string snapshot) {
  std::lock_guard<std::mutex> lg(mtx);
  std::ofstream outfile;
  m_raftStateOutStream << raftState;
  m_snapshotOutStream << snapshot;
}

std::string Persister::ReadSnapshot() {
  std::lock_guard<std::mutex> lg(mtx);
  if (m_snapshotOutStream.is_open()) {
    m_snapshotOutStream.close();
  }
  // 销毁的时候调用lambda表达式
  Defer ec1([this]() -> void { this->m_snapshotOutStream.open(snapshotFile); });
  std::fstream ifs(snapshotFile, std::ios_base::in);
  if (!ifs.good()) {
    return "";
  }
  std::string snapshot;
  ifs >> snapshot;
  ifs.close();
  return snapshot;
}

void Persister::SaveRaftState(const std::string& data) {
  std::lock_guard<std::mutex> lg(mtx);
  m_raftStateOutStream << data;
}

long long Persister::RaftStateSize() {
  std::lock_guard<std::mutex> lg(mtx);
  return m_raftStateSize;
}

std::string Persister::ReadRaftState() {
  std::lock_guard<std::mutex> lg(mtx);
  std::fstream ifs(raftStateFile, std::ios_base::in);
  if (!ifs.good()) {
    return "";
  }

  std::string state;
  ifs >> state;
  ifs.close();
  return state;
}
