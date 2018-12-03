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

#ifndef RDP_COMM_UTIL_H
#define RDP_COMM_UTIL_H

#include "comm_incl.h"

#define OK 0
#define ERR -1

extern int errno;

namespace rdp_comm {
// zookeeper error code to string
const char *ErrorStr(int code);
// zookeeper event to string
const char *EventStr(int event);
// zookeeper state to string
const char *StateStr(int state);
// get parent path
// example: /node1/node2 return /node1
std::string ParentPath(const std::string &path);
// time_t to string example:2017622-15:37:02
std::string StrTime(time_t *t);

int64_t CharToInt64(const char *p);
// get sub ms
uint64_t GetIntervalMs(const struct timeval *begin, const struct timeval *end);
// get sub us
uint64_t GetIntervalUs(const struct timeval *begin, const struct timeval *end);
// split string in sp
// example:"1/2/3" sp:"/"
// result: 0:1 1:2 2:3
// example:"/1//2/3/" sp:"/"
// result: 0:(empty string) 1:1 2:(empty string) 3:2 4:3 5:(empty string)
void SplitString(const std::string &source, const char *sp,
                 std::vector<std::string> *rs);

// Simple locking wrapper
typedef struct spinlock_t {
  explicit spinlock_t(void) {
    pthread_spin_init(&mutex_, PTHREAD_PROCESS_PRIVATE);
  }
  ~spinlock_t(void) { pthread_spin_destroy(&mutex_); }
  void lock(void) { pthread_spin_lock(&mutex_); }
  void unlock(void) { pthread_spin_unlock(&mutex_); }

 private:
  pthread_spinlock_t mutex_;
} spinlock_t;

template <class L>
class guard {
 public:
  explicit guard(L *l) : l_(l) { l_->lock(); }
  ~guard(void) { l_->unlock(); }

 private:
  L *l_;
};

typedef guard<spinlock_t> lock_guard;

// Simple waiter
struct TimedWaiter {
  explicit TimedWaiter(int n) : timeout_(n) { sem_init(&sem_, 0, 0); }

  ~TimedWaiter() { sem_destroy(&sem_); }

  void Begin() {
    clock_gettime(CLOCK_REALTIME, &ts_);
    ts_.tv_sec += timeout_;
  }

  void End() {
    while (sem_timedwait(&sem_, &ts_) == -1 && errno == EINTR)
      continue; /* Restart if interrupted by handler */
  }

  sem_t sem_;
  int timeout_;
  struct timespec ts_;
};

// Return the error message
extern std::string GetLastErrorMsg(int err);

std::string StrToLower(std::string t);

bool CompareNoCase(const std::string &a, const std::string &b);

int CheckThreadAlive(pthread_t tid);

template <typename T>
class Singleton {
 public:
  static T &GetInstance(void) {
    int ret = 0;
    ret = pthread_once(&once_, &Singleton::Init);
    if (0 != ret) {
      LOG(ERROR) << "pthread_once failed: " << GetLastErrorMsg(ret);
      assert(0);
    }

    assert(val_);
    return *val_;
  }

 private:
  static void Init(void) { val_ = new T(); }

 private:
  static pthread_once_t once_;
  static T *val_;
};

template <typename T>
pthread_once_t Singleton<T>::once_ = PTHREAD_ONCE_INIT;

template <typename T>
T *Singleton<T>::val_ = NULL;

uint64_t TimevalToUs(const struct timeval &t);

std::string FormatTimeval(struct timeval *tv);

}  // namespace rdp_comm

#endif  // RDP_COMM_UTIL_H
