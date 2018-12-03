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

#ifndef _RDP_SYNCER_METRIC_H_
#define _RDP_SYNCER_METRIC_H_

#include "syncer_incl.h"
#include "syncer_app.h"
#include "kafka_producer.h"
#include <rdp-comm/util.h>
#include <rdp-comm/metric_reporter.h>

namespace syncer {

struct RDPSyncerMetric {
  explicit RDPSyncerMetric(void);
  ~RDPSyncerMetric(void);

  // 连接MySQL Master的次数
  rdp_comm::CounterMetric* connect_master_count_metric_;
  // 源MySQL集群切换的次数
  rdp_comm::CounterMetric* mysql_switch_count_metric_;
  // 验证MySQL fencing token失败次数 
  rdp_comm::CounterMetric* token_illegal_count_metric_;
  // 本次启动后，处理事务的计数
  rdp_comm::CounterMetric* total_trx_count_metric_;
  // 本次启动后，处理消息的计数
  rdp_comm::CounterMetric* total_msg_count_metric_;
  // 本次启动后收到的binlog总数据量
  rdp_comm::CounterMetric* binlog_received_count_metric_;
  // 本次启动后收到的heartbeat总数据量
  rdp_comm::CounterMetric* binlog_heartbeat_received_count_metric_;

  // 最近一个接收到的binlog event与master的时差
  rdp_comm::CounterMetric* seconds_behind_master_metric_;
  // 最后一个接收到的binlog evnet的时间(rdp所在的时间)
  rdp_comm::CounterMetric* last_event_timestamp_metric_;
  // io线程的状态,-1表示disconnected, 0表示connecting, 2表示connected
  rdp_comm::CounterMetric* io_thread_state_metric_;
  // io线程的流入速率, 单位 bytes/s
  rdp_comm::MeterMetric* in_rate_metric_;
  rdp_comm::MeterMetric* out_rate_metric_;


  // rdp 与master之间binlog延迟的数据量 
  //rdp_comm::CounterMetric* binlog_delaysize_count_metric_;
  // rdp与master之间binlog延迟秒数
  //rdp_comm::CounterMetric* binlog_delaytime_count_metric_;
  // 本次启动后，写到KAFKA的事务计数(接收到Response)
  rdp_comm::CounterMetric* total_done_trx_count_metric_;
  // 本次启动后，写到KAFKA的事务计数(未接收到Response)
  rdp_comm::CounterMetric* total_sent_trx_count_metric_;
  // 本次启动后，写到KAFKA的Message计数(接收到Response)
  rdp_comm::CounterMetric* total_done_msg_count_metric_;
  // 本次启动后，写到KAFKA的Message计数(未接收到Response)
  rdp_comm::CounterMetric* total_sent_msg_count_metric_;
  // 接收到write kafka response最后1000个message的rt的平均时延
  rdp_comm::AvgMetric* write_msg_avg_metric_;

  // pending在bounded buffer中的事务计数
  rdp_comm::CounterMetric* pending_trx_count_metric_;
  // pending在bounded buffer中的msg计数
  rdp_comm::CounterMetric* pending_msg_count_metric_;
  // 串行执行的次数
  rdp_comm::CounterMetric* serial_exec_count_metric_;
  // 并行执行时遇到等待空闲线程的次数
  //rdp_comm::CounterMetric* parallel_waiting_count_metric_;
  // 并行执行时无需等待空闲线程的次数
  //rdp_comm::CounterMetric* parallen_free_count_metric_;
  
  // kafka producer write transaction tps
  rdp_comm::MeterMetric* producer_trans_tps_metric_;
  // encoded map messages count
  rdp_comm::CounterMetric* encoded_msg_count_metric_;
  // kafka producer wait write response pkgs count
  rdp_comm::CounterMetric* wait_rsp_pkgs_count_metric_;
  // kafka producer write messages tps
  rdp_comm::MeterMetric* producer_msg_tps_metric_;

  rdp_comm::MeterMetric* parser_event_eps_;
  rdp_comm::MeterMetric* parser_trans_tps_;
  rdp_comm::AvgMetric* parser_trans_avg_proc_time_;
  // 本次启动后，不启用分包且事务超过包大小限制而被过滤的事务个数统计
  rdp_comm::CounterMetric* filte_trx_count_metric_;
};

extern RDPSyncerMetric* g_syncer_metric;

} // namespace syncer

#endif // _RDP_SYNCER_METRIC_H_

