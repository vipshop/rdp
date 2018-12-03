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

#include "signal_handler.h"

namespace rdp_comm {
namespace signalhandler {
// In Linux, we do NOT wish signal to be dispatched to random
// Threads, because in that way all running threads should handle
// signal carefully. So we intend to handle signal in a separate
// Signal thread called 'SigHanlder';
// https://linux.die.net/man/7/pthreads pthread function never set errno
volatile bool g_running_flag = true;
static QuitCoordinator g_quit_coordinator = NULL;
static sigset_t g_block_set;
static spinlock_t g_map_lock;
// Thread name to thread data mapping
typedef std::map<std::string, ThreadMeta *> ThreadsMap;
static ThreadsMap g_threads_map;

// resources metric interval time ms
static uint32_t g_metric_interval_ms = DEF_METRIC_MS;

// if Wait notify g_checkthd_cond and IsRunning, meaning some threads exit, so
// exit program
static pthread_mutex_t g_checkthd_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_checkthd_cond = PTHREAD_COND_INITIALIZER;

void Handler(int sig_num);

// SIGUSR1 is used to interrupt blocking system call
// @ Most library call & system call are NOT signal safe
static void Sigusr1SignalHandler(int sig_num) {}

void MyQuitCoordinator(void);

// Check all g_threads_map threads alive, if one thread exit, exit program
// timing metric resource
void *CheckThdAndMetricRs(void *args);

// Block signals in main thread, other threads created by main()
// will inherit a copy of the signal mask;
// Gracefully quit:
// 'kill pid' -> 'signal thread' -> 'pthread_kill(other threads, SIGUSR1)'
// -> 'other thread interrupt by signal' -> 'check flag' -> 'quit'
void Install(void) {
  struct sigaction sa;
  (void)sigemptyset(&sa.sa_mask);

  // Ignore SIGPIPE and SIGALRM
  sa.sa_flags = 0;
  sa.sa_handler = SIG_IGN;
  (void)sigaction(SIGPIPE, &sa, NULL);
  (void)sigaction(SIGALRM, &sa, NULL);

  // SIGUSR1 is used to interrupt blocking system call
  sa.sa_handler = Sigusr1SignalHandler;
  (void)sigaction(SIGUSR1, &sa, NULL);

  // Block SIGQUIT, SIGHUP and SIGTERM.
  // The signal handler thread does sigwait() on these
  sigemptyset(&g_block_set);
  sigaddset(&g_block_set, SIGQUIT);
  sigaddset(&g_block_set, SIGHUP);
  sigaddset(&g_block_set, SIGTERM);
  sigaddset(&g_block_set, SIGTSTP);
  sigaddset(&g_block_set, SIGINT);

  int ret = 0;
  if ((ret = pthread_sigmask(SIG_BLOCK, &g_block_set, NULL)) != 0) {
    LOG(ERROR) << "pthread_sigmask failed: (" << GetLastErrorMsg(ret) << ").";
    assert(0);
  }
}

// Individual signal handling thread
void *Run(void *args) {
  int sig = 0, rc = 0;

  pthread_detach(pthread_self());
  pthread_setname_np(pthread_self(), "signal.thread");
  while (g_running_flag) {
    rc = sigwait(&g_block_set, &sig);
    if (rc != -1) {
      Handler(sig);
    } else {
      LOG(ERROR) << "sigwait failed: (" << GetLastErrorMsg(rc) << ").";
      assert(0);
    }
  }

  return NULL;
}

// Coordinate the order other threads quit, overwrite this function
// if you will create threads dynamically or create thread with detached attr.
void DefaultQuitCoordinator(void) {
  int ret = 0, retry_count = 0;
  const int wait_ms = 1000;
  static const int MAX_RETRY_TIMES = 3;
  struct timespec ts;

  lock_guard guard(&g_map_lock);
  // After signal thread receive signal, we try to stop user threads
  // by sending SIGUSR1 to each threads, the order is defined via
  // 'thd_add', Any system call should handle errno = EINTR; Or else
  // we need wait for a timeout interval.
  ThreadsMap::iterator iter = g_threads_map.begin();
  for (; iter != g_threads_map.end(); ++iter) {
    LOG(INFO) << "Stopping thread [" << iter->first << "] ...";
    // pthread_kill is async directed to target thread;
    // use pthread_kill(tid, 0) to check the existence of target thread;
    // If target thread is still alive after a timeout, send a pthread_cannel,
    // in this case, do the most necessary cleanup handling via:
    // pthread_cleanup_push / pthread_cleanup_pop
    retry_count = 0;
    ThreadMeta *meta = iter->second;
    assert(meta);
    do {
      if (0 != (ret = pthread_kill(meta->GetTID(), SIGUSR1))) {
        LOG(ERROR) << "pthread_kill failed: (" << GetLastErrorMsg(ret) << ").";
        if (ESRCH == ret) break;
      }

      meta->GetCondData()->Signal();
      // Skip to join main thread
      if (0 == strcasecmp("main.thread", iter->first.c_str())) {
        while (-1 == usleep(1000 * wait_ms) && EINTR == errno) {
        }
        continue;
      }

      // kill & join with timeout and retry
      if (-1 == clock_gettime(CLOCK_REALTIME, &ts)) {
        LOG(ERROR) << "clock_gettime failed: (" << GetLastErrorMsg(ret) << ").";
        while (-1 == usleep(1000 * wait_ms) && EINTR == errno) {
        }
      } else {
        ts.tv_sec += wait_ms / (time_t)1E3;
        ts.tv_nsec += (long)wait_ms % (long)1E3 * (long)1E6;
        if (ts.tv_nsec >= (long)1E9) {
          ts.tv_sec += 1;
          ts.tv_nsec -= (long)1E9;
        }

        if (0 == (ret = pthread_timedjoin_np(meta->GetTID(), NULL, &ts))) {
          break;
        } else {
          if (EINVAL == ret || ESRCH == ret)  // Not exist or not joinable
            break;
          LOG(INFO) << "Thread [" << iter->first << "] is busy: ("
                    << GetLastErrorMsg(ret) << ").";
        }
      }
    } while (++retry_count < MAX_RETRY_TIMES);

    // SIGUSR1 make no sense, cancel forcedly
    if (retry_count >= MAX_RETRY_TIMES) {
      LOG(ERROR) << "Cancel thread [" << iter->first << "].";
      pthread_cancel(meta->GetTID());
    }
  }  // for
}

// Setup process wide signal mark and spawn signal handling thread
void StartSignalHandler(QuitCoordinator qc, uint32_t metric_interval_ms) {
  if (NULL != qc) {
    g_quit_coordinator = qc;
  } else {
    g_quit_coordinator = DefaultQuitCoordinator;
  }

  // set metric interval time
  g_metric_interval_ms = metric_interval_ms;

  // Spawn internal signal handler thread
  pthread_t tid;
  Install();
  pthread_create(&tid, NULL, Run, NULL);

  // Spawn check all thread alive and metric resource thread
  pthread_t ck_metric_tid;
  pthread_create(&ck_metric_tid, NULL, CheckThdAndMetricRs, NULL);
}

// Internal signal handler
void Handler(int sig_num) {
  char thd_name[16] = {0x0};
  pthread_getname_np(pthread_self(), thd_name, sizeof(thd_name));
  LOG(INFO) << "Thread [" << thd_name << "] receive signal: ("
            << strsignal(sig_num) << ").";

  switch (sig_num) {
    case SIGQUIT:
    case SIGHUP:
    case SIGTERM:
    case SIGTSTP:
    case SIGINT:
      g_running_flag = false;
      if (NULL != g_quit_coordinator) {
        (*g_quit_coordinator)();
      }
      break;

    default:
      break;
  }
}

// Add a thread that need coordination when a signal received
void AddThread(pthread_t tid, const char *thd_name) {
  // Limitation induce by 'pthread_setname_np'
  assert(strlen(thd_name) <= 15);
  pthread_setname_np(pthread_self(), thd_name);

  ThreadMeta *meta = new ThreadMeta(pthread_self());
  assert(meta);
  lock_guard guard(&g_map_lock);
  g_threads_map.insert(ThreadsMap::value_type(std::string(thd_name), meta));
}

// If a user thread quit a thread, call this function
// to remove thread entry from mapping
void DelThread(pthread_t tid) {
  lock_guard guard(&g_map_lock);
  ThreadsMap::iterator iter = g_threads_map.begin();
  for (; iter != g_threads_map.end(); ++iter) {
    if (pthread_equal(iter->second->GetTID(), tid)) {
      delete iter->second;
      iter->second = NULL;
      g_threads_map.erase(iter);
      return;
    }
  }
}

// User threads that requires graceful quit should do waiting in
// its own thread function, cleanup resources after this sync point;
// If user thread register cleanup handler via pthread_cleanup_push/pop,
// Call pthread_cleanup_pop right after 'Wait()'
void Wait() {
  ThreadMeta *meta = NULL;
  pthread_t tid = pthread_self();
  g_map_lock.lock();
  ThreadsMap::iterator iter = g_threads_map.begin();
  for (; iter != g_threads_map.end(); ++iter) {
    if (pthread_equal(iter->second->GetTID(), tid)) {
      meta = iter->second;
      DLOG(INFO) << "Thread PID: " << tid << ", Name: " << iter->first
                 << ", Normal Exit";
      break;
    }
  }
  g_map_lock.unlock();

  if (NULL != meta) meta->GetCondData()->Wait();

  // notify g_checkthd_cond, thread exit
  pthread_mutex_lock(&g_checkthd_mutex);
  pthread_cond_signal(&g_checkthd_cond);
  pthread_mutex_unlock(&g_checkthd_mutex);
}

// State if process wide flag is true
bool IsRunning(void) { return g_running_flag; }

// Customized quit coordinator can retrieve thread data by thread name
// Return:
// NULL - specified thread NOT found; Or else thread meta
ThreadMeta *GetThreadMeta(const char *thd_name) {
  lock_guard guard(&g_map_lock);
  ThreadsMap::iterator iter = g_threads_map.find(thd_name);
  if (iter != g_threads_map.end()) {
    return iter->second;
  } else {
    LOG(ERROR) << "Thread [" << thd_name << "] NOT found.";
    return NULL;
  }
}

// Coordinate the order other threads quit, overwrite this function
// if you will create threads dynamically or create thread with detached attr.
void MyQuitCoordinator(void) {
  int ret = 0, retry_count = 0;
  const int wait_ms = 1000;
  static const int MAX_RETRY_TIMES = 3;
  struct timespec ts;

  // First ckmetric.thd exit stop metric, program exit, no more need metric
  // Second coord.thd exit make sure no transaction give to parser.thd,
  // parser.thd number is configureable, so not set here.
  // Third producer.thd exit second, make sure no message left in
  // encoded_msg_map.
  // Fourth checkpoint.thd exit make sure gtidset and offset info put in update
  // zookeeper queue
  // Fifth zkconfig.thd exit make sure all update zookeeper info saved
  // Sixth election.thd exit left node become slave
  // At last main.thread
  const char *thd_arr[] = {"ckmetric.thd",   "coord.thread", "producer.thd",
                           "checkpoint.thd", "zkconfig.thd", "election.thd",
                           "main.thread"};

  lock_guard guard(&g_map_lock);

  // After signal thread receive signal, we try to stop user threads
  // by sending SIGUSR1 to each threads, the order is defined via
  // 'thd_add', Any system call should handle errno = EINTR; Or else
  // we need wait for a timeout interval.
  ThreadsMap::iterator iter;
  for (size_t i = 0; i < sizeof(thd_arr) / sizeof(thd_arr[0]); i++) {
    iter = g_threads_map.find(thd_arr[i]);
    if (iter == g_threads_map.end()) {
      LOG(ERROR) << "Thread [" << thd_arr[i] << "] NOT found.";
      continue;
    }

    LOG(INFO) << "Stopping thread [" << thd_arr[i] << "] ...";
    // pthread_kill is async directed to target thread;
    // use pthread_kill(tid, 0) to check the existence of target thread;
    // If target thread is still alive after a timeout, send a pthread_cannel,
    // in this case, do the most necessary cleanup handling via:
    // pthread_cleanup_push / pthread_cleanup_pop
    retry_count = 0;
    ThreadMeta *meta = iter->second;
    assert(meta);
    do {
      if (0 != (ret = pthread_kill(meta->GetTID(), SIGUSR1))) {
        LOG(ERROR) << "pthread_kill failed: (" << GetLastErrorMsg(ret).c_str()
                   << ").";
        if (ESRCH == ret) break;
      }

      meta->GetCondData()->Signal();

      // Skip to join main thread
      if (0 == strcasecmp("main.thread", thd_arr[i])) {
        while (-1 == usleep(1000 * wait_ms) && EINTR == errno) {
        }
        continue;
      }

      // kill & join with timeout and retry
      if (-1 == clock_gettime(CLOCK_REALTIME, &ts)) {
        LOG(ERROR) << "clock_gettime failed: (" << GetLastErrorMsg(ret).c_str()
                   << ").";
        while (-1 == usleep(1000 * wait_ms) && EINTR == errno) {
        }
      } else {
        ts.tv_sec += wait_ms / (time_t)1E3;
        ts.tv_nsec += (long)wait_ms % (long)1E3 * (long)1E6;
        if (ts.tv_nsec >= (long)1E9) {
          ts.tv_sec += 1;
          ts.tv_nsec -= (long)1E9;
        }

        if (0 == (ret = pthread_timedjoin_np(meta->GetTID(), NULL, &ts))) {
          break;
        } else {
          if (EINVAL == ret || ESRCH == ret)  // Not exist or not joinable
            break;
          LOG(INFO) << "Thread [" << thd_arr[i] << "] is busy: ("
                    << GetLastErrorMsg(ret).c_str() << ").";
        }
      }
    } while (++retry_count < MAX_RETRY_TIMES);

    // SIGUSR1 make no sense, cancel forcedly
    if (retry_count >= MAX_RETRY_TIMES) {
      LOG(ERROR) << "Cancel thread [" << thd_arr[i] << "].";
      pthread_cancel(meta->GetTID());
    }
  }
}

static void MetricResource(void) {}

static bool CheckAllThdAlive(void) {
  lock_guard guard(&g_map_lock);

  int ret = 0;
  bool rs = true;
  ThreadMeta *meta = NULL;

  ThreadsMap::iterator iter = g_threads_map.begin();
  for (; iter != g_threads_map.end(); ++iter) {

    meta = iter->second;
    assert(meta);

    // check thread alive
    ret = pthread_kill(meta->GetTID(), 0);
    switch (ret) {
      // thread alive
      case 0:
        // DLOG(INFO) << "Thread PID: " << meta->GetTID() << ", Name: " << iter->first << ", Alive";
        rs = true;
        break;

      // thread not exists or exit
      case ESRCH:
        LOG(ERROR) << "Thread PID: " << meta->GetTID()
                   << ", Name: " << iter->first << ", Exit Or Not Exists";
        rs = false;
        break;

      // invalid signal
      case EINVAL:
      default:
        LOG(ERROR) << "pthread_kill failed: (" << GetLastErrorMsg(ret).c_str()
                   << ").";
        // ignore this error
        rs = true;
        break;
    }
    // if some threads exit, return now
    if (!rs) {
      return rs;
    }
  }

  return rs;
}

void *CheckThdAndMetricRs(void *args) {

  pthread_detach(pthread_self());

  char thd_name[16] = {0x0};
  snprintf(thd_name, sizeof(thd_name), "ckmetric.thd");
  AddThread(pthread_self(), thd_name);

  struct timeval now;
  struct timeval pre;
  struct timespec outtime;
  int rs = 0;

  // sign this thread exit is some threads exit or normal exit
  bool except_flag = false;

  gettimeofday(&pre, NULL);

  while (g_running_flag) {

    gettimeofday(&now, NULL);

    // wait g_metric_interval_ms
    outtime.tv_sec = now.tv_sec + (int)(g_metric_interval_ms / 1000);
    outtime.tv_nsec =
        (now.tv_usec + (long)(g_metric_interval_ms % 1000) * 1000) * 1000;

    pthread_mutex_lock(&g_checkthd_mutex);
    // wait
    rs = pthread_cond_timedwait(&g_checkthd_cond, &g_checkthd_mutex, &outtime);

    // signal thread let this thread exit
    if (!g_running_flag) {
      pthread_mutex_unlock(&g_checkthd_mutex);
      // break while, exit thread
      break;
    }

    switch (rs) {
      // g_checkthd_cond signal
      case 0:
        // some threads exit, exit all threads
        g_running_flag = false;
        // let others thread exit
        if (NULL != g_quit_coordinator) {
          (*g_quit_coordinator)();
        }
        // exception exit
        except_flag = true;
        break;

      // outtime timeout
      case ETIMEDOUT:
        // maybe thread down and not call Wait, so check all threads still alive
        if (!CheckAllThdAlive()) {
          // some threads exit, exit all threads
          g_running_flag = false;
          // let others thread exit
          if (NULL != g_quit_coordinator) {
            (*g_quit_coordinator)();
          }
          // exception exit
          except_flag = true;
        }
        break;

      case EINVAL:
      // The mutex was not owned by the current thread at the time of the
      // call.
      // this should not happend
      case EPERM:
      // should not reach default
      default:
        LOG(ERROR) << "CheckThdAndMetricRs Pthread_cond_timedwait Return: "
                   << rs << ", Error: " << rdp_comm::GetLastErrorMsg(errno);
        break;
    }
    pthread_mutex_unlock(&g_checkthd_mutex);

    // metric resource at lease more then g_metric_interval_ms, but short then
    // double g_metric_interval_ms
    // if g_running_flag == false, metric thread had exit
    if (g_running_flag &&
        rdp_comm::GetIntervalMs(&pre, &now) >= g_metric_interval_ms) {
      // metric resource
      MetricResource();
      pre = now;
    }
  }

  // exception exit program
  if (except_flag) exit(0);
}

}  // namespace signalhandler
}  // namespace rdp_comm
