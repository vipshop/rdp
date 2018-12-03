//
//Copyright 2018 vip.com.
//
//Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
//the License. You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
//an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
//specific language governing permissions and limitations under the License.
//

//#include "unistd.h"
#include <zookeeper/zookeeper.h>
#include <string>
#include <string.h>
#include <assert.h>
#include <unistd.h>


std::string g_brokers;
char g_cmd;
std::string g_path;
std::string g_data;

#define BUF_LEN (1*1024*1024)

int Create(zhandle_t *zk_handle) {
  int ret = zoo_create(zk_handle, g_path.c_str(), g_data.c_str(), g_data.size(), &ZOO_OPEN_ACL_UNSAFE, 0, NULL, 0);
  if (ZOK != ret) {
    fprintf(stderr, "%d: %s\n", ret, zerror(ret));
    return -1;
  }

  return 0;
}

int Delete(zhandle_t *zk_handle) {
  int ret = zoo_delete(zk_handle, g_path.c_str(), -1);
  if (ZOK != ret) {
    fprintf(stderr, "%d: %s\n", ret, zerror(ret));
    return -1;
  }

  return 0;
}

int Get(zhandle_t *zk_handle) {
  int buf_len = BUF_LEN;
  char *buf = new char[buf_len];
  assert(buf);
  memset(buf, 0, buf_len);
  struct Stat node_stat;
  int ret = zoo_get(zk_handle, g_path.c_str(), 0, buf, &buf_len, &node_stat);
  if (buf_len > BUF_LEN) {
    fprintf(stderr, "buf is too small\n");
    goto err_t;
  }
  if (ZOK != ret) {
    fprintf(stderr, "%d: %s\n", ret, zerror(ret));
    goto err_t;
  }

  fprintf(stdout, "%s\n", buf);
  delete buf;
  return 0;
  
err_t:
  delete buf;
  return -1;
}

int List(zhandle_t *zk_handle) {
  struct String_vector children;
  int ret = zoo_get_children(zk_handle, g_path.c_str(), 0, &children);
  if (ZOK != ret) {
    fprintf(stderr, "%d: %s\n", ret, zerror(ret));
    return -1;
  }

  for (int32_t i = 0; i < children.count; i++) {
    fprintf(stdout, "%s\n", children.data[i]);
  }

  deallocate_String_vector(&children);

  return 0;
}

int Set(zhandle_t *zk_handle) {
  int ret = zoo_set(zk_handle, g_path.c_str(), g_data.c_str(), g_data.size(), -1);
  if (ZOK != ret) {
    fprintf(stderr, "%d: %s\n", ret, zerror(ret));
    return -1;
  }

  return 0;
}

int Execute() {
  zoo_set_debug_level(ZOO_LOG_LEVEL_ERROR);
  zhandle_t *zk_handle = zookeeper_init(g_brokers.c_str(), NULL, 30000, 0, NULL, 0);
  if (NULL == zk_handle) {
    fprintf(stderr, "Error connect to zookeeper servers\n");
    return -1;
  }

  int ret = 0;
  switch(g_cmd) {
    case 'c': {
      ret = Create(zk_handle);
      break;
    }
    case 'd': {
      ret = Delete(zk_handle);
      break;
    }
    case 'g': {
      ret = Get(zk_handle);
      break;
    }
    case 'l': {
      ret = List(zk_handle);
      break;
    }
    case 's': {
      ret = Set(zk_handle);
      break;
    }
    default: {
      fprintf(stderr, "Unkown command: %c\n", g_cmd);
      ret = -1;
      break;
    }
  }

  zookeeper_close(zk_handle);
  return ret;
}

int PrintHelp(){
  printf("./zk -b xxx -[cdgs] path [data]\n");
  printf("./zk -h\n");
  printf("-b brokers      zookeeper brokers\n");
  printf("-c path data    create node \n");
  printf("-d path         delete node \n");
  printf("-g path         get node \n");
  printf("-l path         list children node \n");
  printf("-s path data    set node data \n");
  printf("\n");

  printf("example: ./zk -b 127.0.0.1:2181,127.0.0.2:2181,127.0.0.3:2181 -c /test_node \"helloworld\" \n");

  return 0;
}

int main(int argc, char **argv) {
  int opt;
  while ((opt = getopt(argc, argv, "b:c:d:g:l:s:h")) != -1) {
    switch (opt) {
      case 'b':
        g_brokers = optarg;
        break;
      case 'c':
        g_cmd  = opt;
        g_path = optarg;
        g_data = argv[optind];
        break;
      case 'd':
        g_cmd = opt;
        g_path = optarg;
        break;
      case 'g':
        g_cmd = opt;
        g_path = optarg;
        break;
      case 'l':
        g_cmd = opt;
        g_path = optarg;
        break;
      case 's':
        g_cmd = opt;
        g_path = optarg;
        g_data = argv[optind];
        break;
      case 'h':
        return PrintHelp();
      default:
        break;
    }
  }

  if (optind > argc) {
    fprintf(stderr, "Expected argument after options\n");
    exit(EXIT_FAILURE);
  }

  if (g_brokers.empty() || g_cmd == '\0') {
    PrintHelp();
    exit(EXIT_FAILURE);
  }

  return Execute();
}
