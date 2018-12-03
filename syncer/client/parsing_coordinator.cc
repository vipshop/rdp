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

#include <rdp-comm/signal_handler.h>
#include <rdp-comm/logger.h>

#include "parsing_coordinator.h"
#include "bounded_buffer.h"
#include "log_event_parser.h"
#include "parsing_worker.h"
#include "syncer_app.h"
#include "rdp_syncer_metric.h"
#include "checkpoint.h"

namespace syncer {

typedef std::vector<CoordinatorInfo *> Participator;
static Participator participator;
static WorkerInfo *workers;

// Get one of idle thread
// TODO: compare enqueue timestamp ? earliest timestamp wins!
static int GetIdlePartitionId(void) {
  int idle_part_id = -1;
  for (register size_t i = 0; i < participator.size(); i++) {
    if (participator[i]->IsIdle()) {
      idle_part_id = i;
      return idle_part_id;
    }
  }

  return idle_part_id;
}

// Coordinlate thread cleanup handler
static void CoordThreadCleanupHandler(void *arg) {}

// Coordinlate thread
void *ParsingCoordinatorThread(void *args) {
  PthreadCall("pthread_detach", pthread_detach(pthread_self()));
  rdp_comm::signalhandler::AddThread(pthread_self(), "coord.thread");
  pthread_cleanup_push(CoordThreadCleanupHandler, NULL);

  CheckPoint *checkpoint = &rdp_comm::Singleton<CheckPoint>::GetInstance();

  size_t idle_partition = 0;
  TrxMsg *trx = NULL;
  string last_dml_gtid;
  // Enter parse loop infinitely, for each binlog file
  while (rdp_comm::signalhandler::IsRunning()) {
    // Get an entry blockingly
    trx = (TrxMsg *)g_trx_bounded_buffer->Pop();
    if (NULL == trx) break;

    g_syncer_metric->total_trx_count_metric_->AddValue(1);
    STAT_TIME_POINT(STAT_T4, trx->seq_no, trx->stat_point);

    // Do coordinlate by transaction type
    if (TRX_DDL == trx->trx_opt) {  // DDL
      RDP_LOG_WARN << "Wait gtid to be acked by kafka: " << last_dml_gtid;
      checkpoint->WaitGtidAcked(last_dml_gtid);
      RDP_LOG_WARN << "Wait gtid to be acked succeed!";
    
      // Waiting one of parsing thread finish its work, since there is only
      // one coordinate thread, after awake there is at least one idle thread
      // can be assigned (#1 Wait) --> (#2 Exec) --> (#3 Wait)
      workers->WaitAllTasksDone();

      int n = GetIdlePartitionId();
      RDP_LOG_DBG << "Get an idle thread "<< n+1 << " for DDL";
      assert(n >= 0);
      idle_partition = n;
      participator[idle_partition]->AssignTask(trx);

      workers->WaitAllTasksDone();
      g_syncer_metric->serial_exec_count_metric_->AddValue(1);
      RDP_LOG_WARN << "Serial execution done!";
    } else {  // DML or Others
      // Record the gtid of last dml
      if (!trx->gtid.empty()) {
        last_dml_gtid = trx->gtid;
      }

      // Assign current task to one of idle thread
      // Case: has idle threads or all thread are busy
      int n = GetIdlePartitionId();
      if (n >= 0) {
        idle_partition = n;
      } else {
        workers->WaitForIdleWorker();
        n = GetIdlePartitionId();
        assert(n >= 0);
        idle_partition = n;
      }

      RDP_LOG_DBG << "Get an idle thread " << idle_partition + 1 << " for DML";
      participator[idle_partition]->AssignTask(trx);
    }

    STAT_TIME_POINT(STAT_T5, trx->seq_no, trx->stat_point);
  }

  // Quit coordinate thread from signal thread
  rdp_comm::signalhandler::Wait();
  RDP_LOG_DBG << "Quit coord.thread";
  pthread_cleanup_pop(1);
  return NULL;
}

// Start a group of threads to parsing binlog events
bool StartParsingWorkers(void) {
  AppConfig *conf = g_syncer_app->app_conf_;
  int thread_num = conf->GetValue<int>("parsing.threads.num", 16);

  workers = new WorkerInfo(thread_num);
  assert(workers);
  // Start a group of parsing threads
  for (register int i = 0; i < thread_num; i++) {
    CoordinatorInfo *ci = new CoordinatorInfo(workers, i + 1);
    if (!StartParsingWorker(ci)) {
      return false;
    }
    participator.push_back(ci);
  }

  return true;
}

// Start parsing workers and coordinator thread
bool StartParsingCoordinator(void) {
  if (!StartParsingWorkers()) return false;

  // Start the unique coordinlate thread
  pthread_t tid;
  PthreadCall("pthread_create", pthread_create(&tid, NULL, ParsingCoordinatorThread, NULL));

  return true;
}

void StopParsingCoordinator(void) {
  if (NULL != workers) {
    delete workers;
    workers = NULL;
  }
}

}  // namespace syncer
