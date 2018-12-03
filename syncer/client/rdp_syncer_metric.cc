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

#include "encoded_msg_map.h"
#include "rdp_syncer_metric.h"
#include <boost/functional/hash.hpp>
#include <boost/bind.hpp>
#include <glog/logging.h>
#include "bounded_buffer.h"

using rdp_comm::MetricReporter;
using rdp_comm::CounterMetric;
using rdp_comm::AvgMetric;
using rdp_comm::MeterMetric;

namespace syncer {

RDPSyncerMetric::RDPSyncerMetric(void) {
  MetricReporter* metric_reporter = g_syncer_app->metric_reporter_;
  connect_master_count_metric_ = new CounterMetric("connect_master_count", 10);
  metric_reporter->RegistMetric(connect_master_count_metric_);

  mysql_switch_count_metric_ = new CounterMetric("mysql_switch_count", 10);
  metric_reporter->RegistMetric(mysql_switch_count_metric_);

  total_trx_count_metric_ = new CounterMetric("total_trx_count", 10);
  metric_reporter->RegistMetric(total_trx_count_metric_);

  total_msg_count_metric_ = new CounterMetric("total_msg_count", 10);
  metric_reporter->RegistMetric(total_msg_count_metric_);

  binlog_received_count_metric_ = new CounterMetric("binlog_received_count", 10);
  metric_reporter->RegistMetric(binlog_received_count_metric_);

  binlog_heartbeat_received_count_metric_ = new CounterMetric("binlog_heartbeat_received_count", 10);
  metric_reporter->RegistMetric(binlog_heartbeat_received_count_metric_);

  total_done_trx_count_metric_ = new CounterMetric("binlog_write_done_count", 10);
  metric_reporter->RegistMetric(total_done_trx_count_metric_);

  total_sent_trx_count_metric_ = new CounterMetric("binlog_sent_count", 10);
  metric_reporter->RegistMetric(total_sent_trx_count_metric_);

  seconds_behind_master_metric_ = new CounterMetric("seconds_behind_master", 10);
  metric_reporter->RegistMetric(seconds_behind_master_metric_);

  last_event_timestamp_metric_ = new CounterMetric("last_event_timestamp", 10);
  metric_reporter->RegistMetric(last_event_timestamp_metric_);

  io_thread_state_metric_ = new CounterMetric("io_thread_state", 10);
  metric_reporter->RegistMetric(io_thread_state_metric_);

  in_rate_metric_ = new MeterMetric("in_rate", 10);
  metric_reporter->RegistMetric(in_rate_metric_);

  out_rate_metric_ = new MeterMetric("out_rate", 10);
  metric_reporter->RegistMetric(out_rate_metric_);

  total_done_msg_count_metric_ = new CounterMetric("msg_write_done_count", 10);
  metric_reporter->RegistMetric(total_done_msg_count_metric_);

  total_sent_msg_count_metric_ = new CounterMetric("msg_sent_count", 10);
  metric_reporter->RegistMetric(total_sent_msg_count_metric_);

  pending_trx_count_metric_ = new CounterMetric("pending_trx_count", 10);
  pending_trx_count_metric_->Use(
      boost::bind(&BoundedBuffer::Size, g_trx_bounded_buffer));
  metric_reporter->RegistMetric(pending_trx_count_metric_);

  pending_msg_count_metric_ = new CounterMetric("pending_msg_count", 10);
  pending_msg_count_metric_->Use(
      boost::bind(&BoundedBuffer::Size, g_event_bounded_buffer));
  metric_reporter->RegistMetric(pending_msg_count_metric_);

  serial_exec_count_metric_ = new CounterMetric("serial_exec_count", 10);
  metric_reporter->RegistMetric(serial_exec_count_metric_);

  write_msg_avg_metric_ = new AvgMetric("write_msg_avg_delay_ms", 10, 1000);
  metric_reporter->RegistMetric(write_msg_avg_metric_);

  producer_trans_tps_metric_ = new MeterMetric("producer_trans_tps", 10);
  metric_reporter->RegistMetric(producer_trans_tps_metric_);

  producer_msg_tps_metric_ = new MeterMetric("producer_msg_tps", 10);
  metric_reporter->RegistMetric(producer_msg_tps_metric_);

  encoded_msg_count_metric_ = new CounterMetric("encoded_msg_count", 10);
  encoded_msg_count_metric_->Use(boost::bind(&EncodedMsgMap::GetMsgSize, &g_encode_msg_map));
  metric_reporter->RegistMetric(encoded_msg_count_metric_);

  wait_rsp_pkgs_count_metric_ = new CounterMetric("wait_rsp_msg_count", 10);
  KafkaProducer *producer = &rdp_comm::Singleton<KafkaProducer>::GetInstance();
  wait_rsp_pkgs_count_metric_->Use(boost::bind(&KafkaProducer::GetWaitRspPkgsSize, producer));
  metric_reporter->RegistMetric(wait_rsp_pkgs_count_metric_);

  parser_event_eps_ = new MeterMetric("parser_event_count", 10);
  metric_reporter->RegistMetric(parser_event_eps_);

  parser_trans_tps_ = new MeterMetric("parser_trans_count", 10);
  metric_reporter->RegistMetric(parser_trans_tps_);

  parser_trans_avg_proc_time_ = new AvgMetric("parser_trans_avg_proc_time", 10, 1000);
  metric_reporter->RegistMetric(parser_trans_avg_proc_time_);

  filte_trx_count_metric_ = new CounterMetric("filte_trx_count", 10);
  metric_reporter->RegistMetric(filte_trx_count_metric_);
}

RDPSyncerMetric::~RDPSyncerMetric(void) {

  #define CLEANUP_OBJ(X) if (X) { metric_reporter->UnRegistMetric(X); delete X; X = NULL; }  

  MetricReporter* metric_reporter = g_syncer_app->metric_reporter_;

  CLEANUP_OBJ(serial_exec_count_metric_);
  CLEANUP_OBJ(pending_msg_count_metric_);
  CLEANUP_OBJ(pending_trx_count_metric_);
  CLEANUP_OBJ(binlog_received_count_metric_);
  CLEANUP_OBJ(total_msg_count_metric_);
  CLEANUP_OBJ(total_trx_count_metric_);
  CLEANUP_OBJ(mysql_switch_count_metric_);
  CLEANUP_OBJ(connect_master_count_metric_);
  CLEANUP_OBJ(total_done_trx_count_metric_);
  CLEANUP_OBJ(total_sent_trx_count_metric_);
  CLEANUP_OBJ(total_done_msg_count_metric_);
  CLEANUP_OBJ(total_sent_msg_count_metric_);
  CLEANUP_OBJ(write_msg_avg_metric_);
  CLEANUP_OBJ(producer_trans_tps_metric_);
  CLEANUP_OBJ(producer_msg_tps_metric_);
  CLEANUP_OBJ(encoded_msg_count_metric_);
  CLEANUP_OBJ(wait_rsp_pkgs_count_metric_);
  CLEANUP_OBJ(parser_event_eps_);
  CLEANUP_OBJ(parser_trans_tps_);
  CLEANUP_OBJ(parser_trans_avg_proc_time_);
  CLEANUP_OBJ(filte_trx_count_metric_);
}

RDPSyncerMetric* g_syncer_metric = NULL;

}  // namespace syncer
