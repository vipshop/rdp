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

#ifndef _COMM_SIGNAL_HANDLER_
#define _COMM_SIGNAL_HANDLER_

#include "comm_incl.h"

#define DEF_METRIC_MS 1000

namespace rdp_comm {
namespace signalhandler {
// In Linux, we do NOT wish signal to be dispatched to random
// Threads, because in that way all running threads should handle
// signal carefully. So we intend to handle signal in a separate
// Signal thread called 'SigHanlder';
// https://linux.die.net/man/7/pthreads pthread function never set errno
typedef void (*QuitCoordinator)(void);

extern void MyQuitCoordinator(void);

typedef struct ThreadCondData {
  sem_t sem;
  explicit ThreadCondData(void) { sem_init(&sem, 0, 0); }
  ~ThreadCondData(void) { sem_destroy(&sem); }
  void Signal(void) { sem_post(&sem); }
  void Wait(void) {
    while (sem_wait(&sem) == -1 && errno == EINTR) {
    }
  }
} ThreadCondData;

typedef struct ThreadMeta {
  pthread_t tid;
  ThreadCondData cond;
  explicit ThreadMeta(pthread_t t) : tid(t) {}
  inline pthread_t GetTID(void) const { return tid; }
  inline ThreadCondData* GetCondData(void) { return &cond; }
} ThreadMeta;

#ifdef __cplusplus
extern "C" {
#endif

// Setup process wide signal mark and spawn signal handling thread,
// optionally a quit coordinator function can be specified to control
// the quit order of a group of registered threads
void StartSignalHandler(QuitCoordinator qc = NULL,
                        uint32_t metric_interval_ms = DEF_METRIC_MS);

// State if process wide flag is true
bool IsRunning(void);

// Add a new thread attached with name to being managed
void AddThread(pthread_t tid, const char* thd_name);
// Remove a thread previous added
void DelThread(pthread_t tid);

// User threads that requires graceful quit should do waiting in
// its own thread function, cleanup resources after this sync point;
// If user thread register cleanup handler via pthread_cleanup_push/pop,
// Call pthread_cleanup_pop right after 'Wait()'
void Wait(void);

// Customized quit coordinator can retrieve thread data by thread name
// Return:
// NULL - specified thread NOT found; Or else thread meta
ThreadMeta* GetThreadMeta(const char* thd_name);

#ifdef __cplusplus
}
#endif

}  // namespace signalhandler
}  // namespace rdp_comm

#endif  // _COMM_SIGNAL_HANDLER_
