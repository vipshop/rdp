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

#ifndef RDP_COMM_INCL_H
#define RDP_COMM_INCL_H

#ifndef _WIN32
#ifndef nullptr
#define nullptr NULL
#endif
#endif

#include <stdio_ext.h>
#include <unistd.h>
#include <sys/inotify.h>
#include <libgen.h>
#include <semaphore.h>
#include <strings.h>
#include <pthread.h>
#include <wait.h>
#include <sys/time.h>
#include <unistd.h>
#include <fcntl.h>   // sync_file_range
#include <malloc.h>  // memalign
#include <sched.h>

// Public header
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <errno.h>
#include <signal.h>
#include <assert.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <limits.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <getopt.h>
#include <string>
#include <vector>
#include <map>
#include <algorithm>
#include <iostream>
#include <sstream>
#include <fstream>
#include <list>
#include <queue>
#include <iterator>
#include <typeinfo>

#include <zookeeper/zookeeper.h>
#include <glog/logging.h>
#include <gflags/gflags.h>
#include <jansson.h>

#include "util.h"


// store all node zk.path relative path
extern const char *g_leader_node_zk_path;

// store master node zk.path relative path
extern const char *g_leader_master_node_zk_path;

#endif  // RDP_COMM_INCL_H
