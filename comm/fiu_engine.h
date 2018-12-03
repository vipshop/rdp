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

#ifndef _FIU_ENGINE_H_
#define _FIU_ENGINE_H_

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <iostream>
#include <limits.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <sys/ipc.h>
#include <sys/mman.h>
#include <sys/shm.h> // shmget
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>

#include <vector>
#include <algorithm>
#include <map>

namespace fiu {

#ifdef __cplusplus
extern "C" {
#endif

// Return True if a sync point is exist
bool IsSyncPointExist(const char *sp);

bool IsSyncPointSet(const char *sp);

// Open shared memory object
bool Open(void);

// Close share memory object
void Close(void);

#ifdef __cplusplus
}
#endif

#ifdef FIU

// TODO: Debug | Release compile flag?
// Initiate DBUG instance, application code can call this macro
// explicitly or invoked from DBUG_* macro
#define DBUG_INIT() \
  do { fiu::Open(); } while (0)

// Shutdown DBUG instance, application code should call this macro
#define DBUG_CLOSE() \
  do { fiu::Close(); } while (0)

// Allows executing a piece of code if the appropriate dbug instruction is set.
#define FIU_EXECUTE_IF(keyword,a1) \
  do { if (fiu::IsSyncPointExist((keyword))) { a1 } } while(0)

#define FIU_EXECUTE(keyword,a1) \
  do { if (fiu::IsSyncPointSet((keyword))) { a1 } } while(0)

// Is used in if expressions and returns val1 if the appropriate dbug
// instruction is set. Otherwise, it returns val2.
#define FIU_EVALUATE_IF(keyword,a1,a2) \
  (fiu::IsSyncPointExist((keyword)) ? (a1) : (a2))

#else

#define DBUG_INIT() void(0)
#define DBUG_CLOSE() void(0)
#define FIU_EXECUTE_IF(keyword,a1) void(0)
#define FIU_EXECUTE(keyword,a1) void(0)
#define FIU_EVALUATE_IF(keyword,a1,a2) void(0)

#endif

} // namespace fiu

#endif // _FIU_ENGINE_H_
