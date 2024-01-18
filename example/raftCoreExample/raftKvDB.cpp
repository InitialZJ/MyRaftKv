#include <unistd.h>

#include <iostream>
#include <random>

#include "kvServer.h"
#include "raft.h"

void ShowArgsHelp() {
  std::cout << "format: command -n <nodeNum> -f <configFileName>" << std::endl;
}

int main(int argc, char** argv) {
  if (argc < 2) {
    ShowArgsHelp();
    exit(EXIT_FAILURE);
  }

	int c = 0;
	int nodeNum = 0;
	return 0;
}
