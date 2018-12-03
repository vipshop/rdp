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

#ifndef _SYNCER_INCL_H_
#define _SYNCER_INCL_H_

#ifndef _WIN32
#ifndef nullptr
#define nullptr NULL
#endif
#endif

#include <stdio_ext.h>
#include <unistd.h>
#include <sys/inotify.h>
#include <arpa/inet.h>
#include <libgen.h>
#include <semaphore.h>
#include <strings.h>
#include <pthread.h>
#include <netdb.h>
#include <resolv.h>
#include <sys/socket.h>
#include <wait.h>
#include <sys/time.h>
#include <poll.h>
#include <sys/ioctl.h>  // ioctl
#include <netinet/tcp.h>
#include <dirent.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>   // sync_file_range
#include <malloc.h>  // memalign
#include <stdarg.h>  // va_*
#include <sched.h>

#ifndef fopen_unlocked
#define fopen_unlocked fopen
#endif

#ifndef fclose_unlocked
#define fclose_unlocked fclose
#endif

#ifndef fseek_unlocked
#define fseek_unlocked fseek
#endif

#ifndef clearerr_unlocked
#define clearerr_unlocked clearerr
#endif

#ifndef ftell_unlocked
#define ftell_unlocked ftell
#endif

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
#include <string>
#include <vector>
#include <map>
#include <algorithm>
#include <iostream>

// Project scope
#include "syncer_consts.h"
#include "syncer_utils.h"

#ifndef NDEBUG
#define private public
#define protected public
#endif

#endif  // _SYNCER_INCL_H_
