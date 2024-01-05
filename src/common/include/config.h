#ifndef CONFIG_H_
#define CONFIG_H_

const bool Debug = true;

const int debugMul = 1;  // MS
const int HeartBeatTimeout = 25 * debugMul;
const int ApplyInterval = 10 * debugMul;

const int minRandomizedElectionTime = 300 * debugMul;
const int maxRandomizedElectionTime = 500 * debugMul;

const int CONSENSUS_TIMEOUT = 500 * debugMul;

#endif  // !CONFIG_H_