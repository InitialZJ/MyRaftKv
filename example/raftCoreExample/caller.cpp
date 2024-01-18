#include <iostream>

#include "clerk.h"
#include "util.h"

int main() {
  Clerk client;
  client.Init("test.conf");
  int count = 500;
  int tmp = count;
  while (tmp--) {
    client.Put("x", std::to_string(tmp));
    std::string get1 = client.Get("x");
    std::cout << "get return: " << get1 << std::endl;
  }

  return 0;
}
