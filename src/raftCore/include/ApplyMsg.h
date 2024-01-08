#ifndef APPLYMSG_H_
#define APPLYMSG_H_

#include <string>

class ApplyMsg {
 public:
  bool CommandValid;
  std::string Command;
  int CommandIndex;
  bool SnapshotValid;
  std::string Snapshot;
  int SnapshotTerm;
  int SnapshotIndex;

 public:
  ApplyMsg()
      : CommandValid(false),
        Command(),
        CommandIndex(-1),
        SnapshotValid(false),
        SnapshotTerm(-1),
        SnapshotIndex(-1) {}
};

#endif  // !APPLYMSG_H_
