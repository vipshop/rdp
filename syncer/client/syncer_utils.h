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

#ifndef _SYNCER_UTILS_H_
#define _SYNCER_UTILS_H_

#include "syncer_incl.h"
#include <glog/logging.h>
//#include <stdint.h>

namespace syncer {

extern void PthreadCall(const char* label, int result); 

// Simple locking wrapper
typedef struct spinlock_t {
  explicit spinlock_t(void) {
    pthread_spin_init(&mutex_, PTHREAD_PROCESS_PRIVATE);
  }
  ~spinlock_t(void) { PthreadCall("pthread_spin_destroy", pthread_spin_destroy(&mutex_)); }
  void lock(void) { PthreadCall("pthread_spin_lock", pthread_spin_lock(&mutex_)); }
  void unlock(void) { PthreadCall("pthread_spin_unlock", pthread_spin_unlock(&mutex_)); }
  pthread_spinlock_t *get(void) { return &mutex_;  }

private:
  pthread_spinlock_t mutex_;
} spinlock_t;

typedef struct mutex_t {
  explicit mutex_t(void) {
    pthread_mutex_init(&mutex_, NULL);
  }
  ~mutex_t(void) { PthreadCall("pthread_mutex_destroy", pthread_mutex_destroy(&mutex_)); }
  void lock(void) { PthreadCall("pthread_mutex_lock", pthread_mutex_lock(&mutex_)); }
  void unlock(void) { PthreadCall("pthread_mutex_unlock", pthread_mutex_unlock(&mutex_)); }
  pthread_mutex_t *get(void) { return &mutex_;  }
private:
  pthread_mutex_t mutex_;

} mutex_t;

template <class L> class guard {
public:
  explicit guard(L *l) : l_(l) { l_->lock(); }
  ~guard(void) { l_->unlock(); }

private:
  L *l_;
};



typedef guard<spinlock_t> lock_guard;
typedef guard<mutex_t> mutex_guard;


typedef struct cond_t {
  explicit cond_t(void) { pthread_cond_init(&cond_, NULL); }
  ~cond_t(void) { PthreadCall("pthread_cond_destroy", pthread_cond_destroy(&cond_)); }
  void broadcast() { PthreadCall("pthread_cond_broadcast", pthread_cond_broadcast(&cond_)); }
  void wait(pthread_mutex_t *mutex) { PthreadCall("pthread_cond_wait", pthread_cond_wait(&cond_, mutex)); }
  int wait(pthread_mutex_t *mutex, int timeout) { 
      struct timespec abstime;
      clock_gettime(CLOCK_REALTIME, &abstime);
      const int64_t kNanoSecondsPerMs = 1000000;
      const int64_t kNanoSecondsPerSecond = 1000000000;
      int64_t nanoseconds = static_cast<int64_t>(timeout * kNanoSecondsPerMs);
      abstime.tv_sec += static_cast<time_t>((abstime.tv_nsec + nanoseconds) / kNanoSecondsPerSecond);
      abstime.tv_nsec = static_cast<long>((abstime.tv_nsec + nanoseconds) % kNanoSecondsPerSecond);
      return pthread_cond_timedwait(&cond_, mutex, &abstime);
  }
private:
  pthread_cond_t cond_;

} cond_t;

// Simple waiter
struct TimedWaiter {
  explicit TimedWaiter(int n) : timeout_(n) { sem_init(&sem_, 0, 0); }

  ~TimedWaiter() { sem_destroy(&sem_); }

  void Begin() {
    clock_gettime(CLOCK_REALTIME, &ts_);
    ts_.tv_sec += timeout_;
  }

  // TODO: EINTR handling if not quitting
  void End() {
#if 0
    while (sem_timedwait(&sem_, &ts_) == -1 && errno == EINTR)
      continue; /* Restart if interrupted by handler */
#else
    if (-1 == sem_timedwait(&sem_, &ts_)) {
      assert(EINTR == errno || ETIMEDOUT);
    }
#endif
  }

  sem_t sem_;
  int timeout_;
  struct timespec ts_;
};

// Return the error message
extern std::string GetLastErrorMsg(int err);

// Synchronously send data with length
extern bool SyncSend(int fd, const uint8_t *data, int32_t size);

/* 解析压缩过的int类型，lenenc-int */
extern bool UnpackLenencInt(char *pBuf, uint32_t &uPos, uint64_t &uValue);

// 检查是否为system schema
extern bool IsSystemSchema(const std::string db_name); 

extern bool IsDataEvent(uint32_t event_type);

extern uint32_t g_max_trans_size;

#define TRUNC_ROWS_EVENT_SIZE (sizeof(PreHeader) + Binary_log_event::ROWS_HEADER_LEN_V2)

bool IsEventTruncated(uint32_t event_type, uint32_t msg_len); 

std::string WriteBits(char *buf, uint32_t buf_size);
uint64_t DecodeBit(unsigned char *b, int length);

void Str2Lower(std::string& str);

// Trim space at both end
std::string Trim(std::string const& source, char const *delims = " \t\r\n");

} // namespace syncer

#endif // _SYNCER_UTILS_H_
