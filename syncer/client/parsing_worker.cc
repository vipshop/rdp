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
#include <boost/lexical_cast.hpp>

#include "syncer_filter.h"
#include "parsing_worker.h"
#include "log_event_parser.h"
#include "encoded_msg_map.h"
#include "syncer_app.h"
#include "split_package.h"
#include "memory_pool.h"
#include "rdp_syncer_metric.h"
#include "rebuild.h"

namespace syncer {

// Parsing thread cleanup handler
static void WorkerThreadCleanupHandler(void *args) {
  CoordinatorInfo *p = (CoordinatorInfo *)args;
  if (NULL != p) {
    RDP_LOG_DBG << "Parsing thread #" << p->thd_no << " cancelled";
    delete p;
    p = NULL;
  }
}

void CleanTrans(rdp::messages::Transaction &transaction) {
  for (int i = 0; i < transaction.events_size(); i++) {
    rdp::messages::Event *event = transaction.mutable_events(i);
    if (IsDataEvent(event->event_type()) && 
        event->rows_size() > 0) {
      return;
    }
    
    if (event->has_ddl() && event->ddl() == rdp::messages::kDDLChanged) {
      return;
    }
  }

  transaction.clear_events();

  return;
}

void CleanTransaction(Transaction *transaction, SeqTransCtx *seq_trans_ctx, bool is_truncated) {
  assert(transaction != NULL);
  assert(seq_trans_ctx != NULL);

  if (is_truncated) {
    RDP_LOG_WARN << " Transaction is truncated, binlog_file_name:"<< transaction->binlog_file_name() 
      << ", postition:" << transaction->position()
      << ", next_binlog_file_name:" << transaction->next_binlog_file_name()
      << ", next_position:" << transaction->next_position();
    ClearTransEventAlarm(transaction);
  }

  if (seq_trans_ctx->enable_split_msg) {
    return;
  }

  if (seq_trans_ctx->enable_trans_clean) {
    CleanTrans(*transaction);
  }

  return;
}

// Parsing thread loop
void *ParsingWorkerThread(void *args) {
  CoordinatorInfo *ci = (CoordinatorInfo *)args;
  assert(ci);

  PthreadCall("pthread_detach", pthread_detach(pthread_self()));
  pthread_cleanup_push(WorkerThreadCleanupHandler, ci);

  char thd_name[16] = {0x0};
  snprintf(thd_name, sizeof(thd_name), "parsing.thd%02d", ci->thd_no);
  rdp_comm::signalhandler::AddThread(pthread_self(), thd_name);

  TrxMsg *trx_msg = NULL;
  EventMsg *em = NULL;
  LogEventParser le_parser;
  SeqTrans *seq_trans = NULL;
  rdp::messages::Transaction *transaction = NULL;

  bool enable_checksum = g_syncer_app->app_conf_->GetValue<string>("checksum.enable", "0").compare("1") == 0 ? true : false;
  bool enable_compress = g_syncer_app->app_conf_->GetValue<string>("compress.enable", "0").compare("1") == 0 ? true : false;
  size_t split_msg_bytes = g_syncer_app->app_conf_->GetValue<size_t>("kafka.producer.split_msg_bytes");
  bool enable_split_msg = g_syncer_app->app_conf_->GetValue<uint32_t>("kafka.producer.enable_split_msg", 0) == 1 ? true : false;
  bool enable_trans_clean = g_syncer_app->app_conf_->GetValue<uint32_t>("kafka.producer.enable_trans_clean", 0) == 1 ? true : false;

  SeqTransCtx *seq_trans_ctx = new SeqTransCtx(split_msg_bytes, 
                                               enable_checksum, 
                                               enable_compress, 
                                               enable_split_msg, 
                                               enable_trans_clean,
                                               2 * 1024 * 1024);

  // Enter parse loop infinitely, for each binlog file
  while (rdp_comm::signalhandler::IsRunning()) {
    transaction = new rdp::messages::Transaction;
    // Get an entry blockingly
    trx_msg = ci->GetTask();
    if (NULL == trx_msg) break;

    timeval start, end;
    gettimeofday(&start, NULL);

    STAT_TIME_POINT(STAT_T6, trx_msg->seq_no, trx_msg->stat_point);

    EventMsg *first_event = NULL, *last_event = NULL;
    PreHeader *ph_tmp = NULL;
    uint32_t event_num = 0;
    string next_binlog_name;
    uint64_t next_binlog_pos;

    // Group commit id
    int64_t last_committed = 0;
    int64_t sequence_number = 0;

    // Parser a transaction
    RDP_LOG_DBG << "---------Thread #" << ci->thd_no 
                << "New (" << (trx_msg->trx_opt == TRX_DDL ? "DDL" : "DML") << ") Transaction("
                << trx_msg->seq_no << ")";

    transaction->set_seq(trx_msg->seq_no);

    std::vector<EventMsg *>::iterator iter = trx_msg->events.begin();
    for (; iter != trx_msg->events.end(); ++iter) {
      em = *iter;
      assert(em);

      // Per log event
      PreHeader *ph = (PreHeader *)em->msg;
      assert(em->len > BINLOG_CHECKSUM_LEN);

      rdp::messages::Event *event = transaction->add_events();
      assert(event);

      uint32_t event_len = em->len;
      if (trx_msg->checksum != binary_log::BINLOG_CHECKSUM_ALG_UNDEF &&  
          (ph->event_type == binary_log::FORMAT_DESCRIPTION_EVENT || 
           trx_msg->checksum != binary_log::BINLOG_CHECKSUM_ALG_OFF) && 
         !IsEventTruncated(ph->event_type, event_len)) {
          // TODO
          event_len = event_len - BINLOG_CHECKSUM_LEN;
      }

      // if parse trx failt, exit thread, check all threads alive thread will
      // catch this event and exit program
      if (ERROR_STOP ==
          le_parser.Parse(em->msg, event_len, em->binlog_fname,
                          trx_msg->trx_opt == TRX_DDL ? true : false, ph, event,
                          transaction->mutable_gtid(),
                          next_binlog_name, next_binlog_pos, 
                          last_committed,sequence_number)) {

        RDP_LOG_ERROR << "Parse Seq No: " << trx_msg->seq_no << ", Failt";
        g_syncer_app->Alarm("Parse transaction Failt");

        goto end;
      }
    }

    first_event = trx_msg->events[0];
    transaction->set_binlog_file_name(first_event->binlog_fname);
    ph_tmp = (PreHeader *)first_event->msg;
    assert(ph_tmp->log_pos > ph_tmp->event_size);
    transaction->set_position(ph_tmp->log_pos - ph_tmp->event_size);

    event_num = trx_msg->events.size();
    assert(event_num > 0);
    last_event = trx_msg->events[event_num - 1];
    transaction->set_next_binlog_file_name(last_event->binlog_fname);
    ph_tmp = (PreHeader *)last_event->msg;
    transaction->set_next_position(ph_tmp->log_pos);
    if (next_binlog_name.size()) {
      transaction->set_next_binlog_file_name(next_binlog_name);
      transaction->set_next_position(next_binlog_pos);
    }
    transaction->set_last_committed(last_committed);
    transaction->set_sequence_number(sequence_number);


    RDP_LOG_DBG << "---------Thread #" <<ci->thd_no 
                << "End (" << (trx_msg->trx_opt == TRX_DDL ? "DDL" : "DML") << ") Transaction"
                << "(" << trx_msg->seq_no << ")";

    STAT_TIME_POINT(STAT_T7, trx_msg->seq_no, trx_msg->stat_point);

    CleanTransaction(transaction, seq_trans_ctx, trx_msg->is_truncated);

    seq_trans = TransactionToSeqTrans(transaction, seq_trans_ctx, trx_msg->seq_no);
    if (seq_trans == NULL) {
      RDP_LOG_ERROR << "Transaction To Seq Trans Failt";
      abort();
    }

    g_encode_msg_map.AddMsg(trx_msg->seq_no, seq_trans);
    seq_trans = NULL;

    STAT_TIME_POINT(STAT_T11, trx_msg->seq_no, trx_msg->stat_point);

    le_parser.clear_tablemap();

    delete transaction;
    transaction = NULL;

    g_syncer_metric->parser_event_eps_->AddValue(trx_msg->events.size());
    g_syncer_metric->parser_trans_tps_->AddValue(1);

    // Delete this entry
    delete trx_msg;
    trx_msg = NULL;
    ci->TaskDone();

    gettimeofday(&end, NULL);
    g_syncer_metric->parser_trans_avg_proc_time_->PushValue(rdp_comm::TimevalToUs(end) - rdp_comm::TimevalToUs(start));
  }

end:

  if (NULL != transaction) {
    delete transaction;
    transaction = NULL;
  }

  if (NULL != trx_msg) {
    delete trx_msg;
    trx_msg = NULL;
  }

  if (NULL != seq_trans) {
    delete seq_trans;
    seq_trans = NULL;
  }

  if (NULL != seq_trans_ctx) {
    delete seq_trans_ctx;
    seq_trans_ctx = NULL;
  }

  ci->TaskDone();

  // Quit parsing from signal thread
  rdp_comm::signalhandler::Wait();
  RDP_LOG_DBG << "Quit " << thd_name;
  pthread_cleanup_pop(1);
  return NULL;
}

// Spawn a new parsing thread
bool StartParsingWorker(CoordinatorInfo *ci) {
  pthread_t tid;
  PthreadCall("pthread_create", pthread_create(&tid, NULL, ParsingWorkerThread, ci));

  return true;
}

void StopParsingWorker(void) {}

}  // namespace syncer
