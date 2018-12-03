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
#include "bounded_buffer.h"
#include "syncer_app.h"
#include "syncer_progress.h"
#include "syncer_def.h"
#include "trx_boundary_parser.h"
#include "syncer_slave.h"
#include <rdp-comm/util.h>
#include <rdp-comm/fiu_engine.h>
#include "rdp_syncer_metric.h"

using namespace rdp_comm;
using namespace fiu;

namespace syncer {

// Group multiple log events to a transaction by string type
struct EventTypeGroup {
  std::string ss;
  EventTypeGroup(void) : ss("") {}

  void Append(const std::string &s) {
    if ("" != ss) {
      ss += " | ";
    }
    ss += s;
  }

  void Append(const char *s, const char *z) {
    if ("" != ss) {
      ss += " | ";
    }
    ss += std::string(s) + "(" + std::string(z) + ")";
  }

  void Reset(void) { ss = ""; }
};


// Boundary parsing thread cleanup handler
static void TrxThreadCleanupHandler(void *arg) {}

// Boundary parsing thread
void *ParsingTrxThread(void *args) {
  PthreadCall("pthread_detach", pthread_detach(pthread_self()));
  rdp_comm::signalhandler::AddThread(pthread_self(), "parsing.trx");
  pthread_cleanup_push(TrxThreadCleanupHandler, NULL);

  // continue transaction seq no
  CheckPoint *checkpoint = &rdp_comm::Singleton<CheckPoint>::GetInstance();
  // checkpoint save last complete pkg transaction seq no, so +1
  uint64_t g_trx_seqno = checkpoint->GetTransSeqNo() + 1;

  //EventTypeGroup ev_group;
  TrxMsg *trx_msg = NULL;
  SyncedProgress *progress = g_syncer_app->synced_progress_;

  // Enter parse loop infinitely, for binlog events stream
  while (rdp_comm::signalhandler::IsRunning()) {
    EventMsg *em = NULL;
    bool encounter_trx_boundary = false;
    bool is_ddl = false;
    const char* event_buf = NULL;
    uint32_t event_len = 0;
    PreHeader *ph = NULL;

    // Get an entry blockingly
    em = (EventMsg*)g_event_bounded_buffer->Pop();
    if (NULL == em) break; 

    g_syncer_metric->total_msg_count_metric_->AddValue(1);
    event_buf = em->msg;
    event_len = em->len;
    ph = (PreHeader *)event_buf;

    // If feed_event detect invalid case, log errors
    if (g_trx_boundary_parser.feed_event(event_buf, event_len, true)) {
      RDP_LOG_ERROR << "Trx boundary parser feed_event!";
      goto err_t;
    }
    encounter_trx_boundary = g_trx_boundary_parser.is_not_inside_transaction();
    is_ddl = g_trx_boundary_parser.is_ddl();
    //ev_group.Append(Log_event::get_type_str((Log_event_type)ph->event_type),
    //              is_ddl ? "DDL" : "DML");

    // Cache latest Gtid
    if (ph->event_type == binary_log::GTID_LOG_EVENT) {
      progress->CacheLatestGtid(event_buf, ph->event_size);

      FIU_EXECUTE_IF("simulate_inside_trx", {
        RDP_LOG_INFO << "+d simulate_inside_trx";
        progress->DumpLatestGtid();
        progress->Dump();
      });
    }

    if (NULL == trx_msg) {
      trx_msg = new TrxMsg(g_trx_seqno);

      STAT_TIME_POINT(STAT_T1, trx_msg->seq_no, trx_msg->stat_point);
    }

    trx_msg->AddEvent(em, is_ddl ? TRX_DDL : TRX_DML);
    // Push a whole transaction events into bounded_buffer
    if (encounter_trx_boundary) {
      // A full transaction had being delivered, update gtidset_executed
      if (!progress->Commit()) {
        RDP_LOG_ERROR << "SyncedProgress Commit failt";
        goto err_t;
      }

      STAT_TIME_POINT(STAT_T2, trx_msg->seq_no, trx_msg->stat_point);
      trx_msg->checksum = g_checksum_alg;
      g_trx_bounded_buffer->Push(trx_msg);
      g_trx_seqno++;
      STAT_TIME_POINT(STAT_T3, trx_msg->seq_no, trx_msg->stat_point);


      trx_msg = NULL;

      //ev_group.Reset();


      g_trx_boundary_parser.reset();
    }

    FIU_EXECUTE_IF("simulate_slow_return", { sleep(5); });    
  } // while (rdp_comm::signalhandler::IsRunning())

err_t:
  progress->ResetLatestGtid();
  if (trx_msg != NULL) {
    delete trx_msg;
    trx_msg = NULL;
  }
  // Quit boundary parsing thread from signal thread
  rdp_comm::signalhandler::Wait();
  RDP_LOG_INFO << "Quit parsing.trx";
  pthread_cleanup_pop(1);
  return NULL;
}

// Start parsing transaction boundary thread
bool StartParsingTrx(void) {
  // Start the boundary parsing thread
  pthread_t tid;
  PthreadCall("pthread_create", pthread_create(&tid, NULL, ParsingTrxThread, NULL));

  return true;
}

void StopParsingTrx(void) {}

}  // namespace syncer
