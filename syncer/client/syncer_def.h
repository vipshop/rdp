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

#ifndef _SYNCER_DEF_H_
#define _SYNCER_DEF_H_

#include "syncer_main.h"
#include "syncer_incl.h"
#include "statistics_writer.h"
#include "memory_pool.h"

namespace syncer {

#pragma pack(push)
#pragma pack(1)

struct EventMsg {
  uint32_t len;
  char *binlog_fname;
  char *msg;

#if 0
  EventMsg(const char *fn, const char *p, uint32_t n) {
    len = n;
    snprintf(binlog_fname, sizeof(binlog_fname), "%s", fn);
    msg = new char[len + 1];
    memcpy(msg, p, len);
    msg[len] = 0x0;
  }

  ~EventMsg(void) {
    delete[] msg;
    msg = NULL;
  }
#endif
};

enum TrxOpt {
  TRX_ERR = 0,
  // If is a DDL transaction
  TRX_DDL = 1,
  // If is a DML transaction
  TRX_DML = 2,
  // If current transaction is ignorable
  TRX_SKIP = 3
};

struct TrxMsg {
  TrxOpt trx_opt;
  // Global sequence no to coordinate parsing order and the order send to Kafka
  uint64_t seq_no;
  std::string gtid;
  std::vector<EventMsg *> events;
  enum_binlog_checksum_alg checksum;

  // 事务中所有event大小总和
  uint32_t len; 

  bool is_truncated;

#ifdef STAT_TIME
  StatPoint *stat_point;
#endif

  TrxMsg(uint64_t sn) : trx_opt(TRX_ERR), seq_no(sn), len(0), is_truncated(false) { 
    events.clear(); 
  }

  ~TrxMsg(void) {
    std::vector<EventMsg *>::iterator iter = events.begin();
    for (; iter != events.end(); ++iter) {
      PreHeader *ph = (PreHeader *)(*iter)->msg;
      if (IsEventTruncated(ph->event_type, (*iter)->len)) {
        free(*iter);
      } else {
        g_memory_pool->free(*iter);
      }
    }
  }

  void AddEvent(EventMsg *e, TrxOpt opt) {
    events.push_back(e);
    if (trx_opt != TRX_DDL) {
      trx_opt = opt;
    }

    len += e->len;
    if (len > g_max_trans_size) {
      truncate_events();
    }
  }

  void truncate_events() {
    for (int64_t i = events.size()-1; i >= 0; i--) {
      EventMsg *e = events[i];
      PreHeader *ph = (PreHeader *)e->msg;

      if (IsDataEvent(ph->event_type)) {
        if (e->len == TRUNC_ROWS_EVENT_SIZE) {
          // All data events have been truncated already
          break;
        }

        // EventMsg + binlog file name(32) + TRUNC_ROWS_EVENT_SIZE + tail'\0'(1) 
        uint32_t tmp_len = sizeof(EventMsg) + 32 + TRUNC_ROWS_EVENT_SIZE;
        char *tmp = (char *)malloc(tmp_len + 1);
        memcpy(tmp , e, tmp_len);
        tmp[tmp_len] = 0x0;

        EventMsg *e_tmp = (EventMsg *)tmp;
        e_tmp->len = TRUNC_ROWS_EVENT_SIZE;
        e_tmp->msg = tmp + sizeof(EventMsg) + 32;

        g_memory_pool->free(e);
        events[i] = e_tmp;
      }
    }

    is_truncated = true;
  }
};


// Track all workers's pending task status
struct WorkerInfo {
  uint64_t pending_tasks_count;
  const uint64_t parallel_workers_count;
  pthread_cond_t empty_cond;
  pthread_mutex_t mutex;

  explicit WorkerInfo(uint64_t thd_num) 
    : pending_tasks_count(0), parallel_workers_count(thd_num) { 
    PthreadCall("pthread_mutex_init", pthread_mutex_init(&mutex, NULL));
    PthreadCall("pthread_cond_init", pthread_cond_init(&empty_cond, NULL));
  }

  ~WorkerInfo(void) { 
    PthreadCall("pthread_cond_destroy", pthread_cond_destroy(&empty_cond));
    PthreadCall("pthread_mutex_destroy", pthread_mutex_destroy(&mutex));
  }

  void PendingTasksInc(void) {
    PthreadCall("pthread_mutex_lock", pthread_mutex_lock(&mutex));
    ++pending_tasks_count;
    PthreadCall("pthread_mutex_unlock", pthread_mutex_unlock(&mutex));
  }

  void PendingTasksDec(void) {
    PthreadCall("pthread_mutex_lock", pthread_mutex_lock(&mutex));
    --pending_tasks_count;
    PthreadCall("pthread_cond_signal", pthread_cond_signal(&empty_cond));
    PthreadCall("pthread_mutex_unlock", pthread_mutex_unlock(&mutex));
  }

  // Wait till one of worker is idle
  void WaitForIdleWorker(void) {
    PthreadCall("pthread_mutex_lock", pthread_mutex_lock(&mutex));
    // pending_tasks_count will NOT > parallel_workers_count
    // if pending_tasks_count = parallel_workers_count, means all workers busy
    while (pending_tasks_count >= parallel_workers_count) {
      PthreadCall("pthread_cond_wait", pthread_cond_wait(&empty_cond, &mutex));
    }
    PthreadCall("pthread_mutex_unlock", pthread_mutex_unlock(&mutex));
  } 
 
  // Wait till all pending tasks done
  void WaitAllTasksDone(void) {
    PthreadCall("pthread_mutex_lock", pthread_mutex_lock(&mutex));
    while (pending_tasks_count > 0) {
      PthreadCall("pthread_cond_wait", pthread_cond_wait(&empty_cond, &mutex));
    }
    PthreadCall("pthread_mutex_unlock", pthread_mutex_unlock(&mutex));
  }
};

// Track per worker's task status and global pending task status
struct CoordinatorInfo {
  TrxMsg *trx;
  uint64_t trx_in;
  uint64_t trx_out;
  const uint32_t thd_no;
  pthread_cond_t idle_cond;
  pthread_cond_t busy_cond;
  pthread_mutex_t mutex;
  WorkerInfo* worker_info;

  explicit CoordinatorInfo(WorkerInfo* wi, uint32_t no) 
  : trx(NULL), trx_in(0), trx_out(0), thd_no(no), worker_info(wi) {
    PthreadCall("pthread_mutex_init", pthread_mutex_init(&mutex, NULL));
    PthreadCall("pthread_cond_init", pthread_cond_init(&idle_cond, NULL));
    PthreadCall("pthread_cond_init", pthread_cond_init(&busy_cond, NULL));
  }

  ~CoordinatorInfo(void) {
    PthreadCall("pthread_cond_destroy", pthread_cond_destroy(&busy_cond));
    PthreadCall("pthread_cond_destroy", pthread_cond_destroy(&idle_cond));
    PthreadCall("pthread_mutex_destroy", pthread_mutex_destroy(&mutex));
  }

  // Coordinator call step-1
  bool IsIdle(void) {
    bool is_idle = false;
    PthreadCall("pthread_mutex_lock", pthread_mutex_lock(&mutex));
    is_idle = (NULL == trx);
    PthreadCall("pthread_mutex_unlock", pthread_mutex_unlock(&mutex));
    return is_idle;
  }

  // Coordinator call step-2
  void WaitTaskDone(void) {
    PthreadCall("pthread_mutex_lock", pthread_mutex_lock(&mutex));
    while (NULL != trx) {
      PthreadCall("pthread_cond_wait", pthread_cond_wait(&idle_cond, &mutex));
    }
    assert(NULL == trx);
    PthreadCall("pthread_mutex_unlock", pthread_mutex_unlock(&mutex));
  }

  // Coordinator call step-3
  void AssignTask(TrxMsg *p) {
    PthreadCall("pthread_mutex_lock", pthread_mutex_lock(&mutex));
    while (NULL != trx) {
      PthreadCall("pthread_cond_wait", pthread_cond_wait(&idle_cond, &mutex));
    }
    assert(NULL == trx);
    trx = p;
    ++trx_in;
    PthreadCall("pthread_cond_signal", pthread_cond_signal(&busy_cond));
    PthreadCall("pthread_mutex_unlock", pthread_mutex_unlock(&mutex));
    // Inc global pending tasks count
    worker_info->PendingTasksInc();
  }

  // Parsing thread(s) call step-1
  TrxMsg *GetTask(void) {
    PthreadCall("pthread_mutex_lock", pthread_mutex_lock(&mutex));
    while (NULL == trx) {
      PthreadCall("pthread_cond_wait", pthread_cond_wait(&busy_cond, &mutex));
    }
    PthreadCall("pthread_mutex_unlock", pthread_mutex_unlock(&mutex));
    assert(trx);
    return trx;
  }

  // Parsing thread(s) call step-2
  void TaskDone(void) {
    PthreadCall("pthread_mutex_lock", pthread_mutex_lock(&mutex));
    trx = NULL;
    ++trx_out;
    PthreadCall("pthread_cond_signal", pthread_cond_signal(&idle_cond));
    PthreadCall("pthread_mutex_unlock", pthread_mutex_unlock(&mutex));
    // Dec global pending tasks count
    worker_info->PendingTasksDec();
  }
};

#pragma pack(pop)

} // namespace syncer

#endif // _SYNCER_DEF_H_
