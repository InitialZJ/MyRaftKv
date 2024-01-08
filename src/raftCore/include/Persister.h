#ifndef PERSISTER_H_
#define PERSISTER_H_

#include <fstream>
#include <mutex>

class Persister {
 public:
  void Save(std::string raftState, std::string snapshot);
  std::string ReadSnapshot();
  void SaveRaftState(const std::string& data);
  long long RaftStateSize();
  std::string ReadRaftState();
  explicit Persister(int me);
  ~Persister();

 private:
  std::mutex mtx;
  std::string m_raftState;
  std::string m_snapshot;
  const std::string raftStateFile;
  const std::string snapshotFile;
  std::ofstream m_raftStateOutStream;
  std::ofstream m_snapshotOutStream;
  long long m_raftStateSize;
};

#endif  // !PERSISTER_H_
