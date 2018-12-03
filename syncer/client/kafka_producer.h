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

#ifndef _KAFKA_PRODUCER_H_
#define _KAFKA_PRODUCER_H_

#define __STDC_FORMAT_MACROS
#include <inttypes.h>

#include "syncer_incl.h"

#include <librdkafka/rdkafkacpp.h>
#include <rdp-comm/util.h>
#include <rdp-comm/threadbase.h>
#include <rdp-comm/metric_reporter.h>

#include "syncer_utils.h"
#include "syncer_main.h"
#include "encoded_msg_map.h"
#include "checkpoint.h"
#include "statistics_writer.h"
#include "split_package.h"
#include "rdp_syncer_metric.h"
#include "mysql_gtidmgr.h"

// default msg_max_bytes_ 500k
#define DEFAULT_MSG_MAX_BYTES 512000

using std::vector;
using std::string;
using std::map;
using std::queue;

using rdp_comm::StateStr;
using rdp_comm::GetIntervalMs;
using rdp_comm::ThreadBase;
using rdp_comm::FormatTimeval;

namespace syncer {

class KfkProducerDRCb : public RdKafka::DeliveryReportCb {
 public:
  void dr_cb(RdKafka::Message &message);
};

class KfkProducerECb : public RdKafka::EventCb {
 public:
  void event_cb(RdKafka::Event &event);
};

class KafkaProducer : public ThreadBase {
 private:
  typedef map<string, SeqKafkaPkg *> KafkaPkgMap;

 public:
  KafkaProducer();

  ~KafkaProducer();

  // \brief Init: check zookeeper connection status and init kafka producer
  //
  // \param brokerlists: kafka brokers list. e.g.:ip:port,ip:port
  // \param topic_name: kafka topic name string
  // \param partitions: kafka topic partition
  // \param required_acks
  // required_acks = 0, which means that the producer never waits for an
  // acknowledgement from the broker (the same behavior as 0.7). This option
  // provides the lowest latency but the weakest durability guarantees (some
  // data will be lost when a server fails).
  // required_acks = 1, which means that the producer gets an acknowledgement
  // after the leader replica has received the data. This option provides
  // better durability as the client waits until the server acknowledges the
  // request as successful (only messages that were written to the now-dead
  // leader but not yet replicated will be lost).
  // required_acks = -1, which means that the producer gets an acknowledgement
  // after all in-sync replicas have received the data. This option provides
  // the best durability, we guarantee that no messages will be lost as long as
  // at least one in sync replica remains.
  //
  // \param batch_num_msg: write kafka batch transactions number
  // \param batch_time_ms: write kafka batch transactions wait max time
  // \param msg_max_bytes: max kafka message size
  // \param msg_map: EncodedMsgMap
  // \param checkpoint: CheckPoint thread
  //
  // \return: true if success else failt
  bool Init(const string &brokerlists, const string &topic_name,
            const vector<int> &partitions, const string &required_acks,
            const uint32_t batch_num_msg, const uint32_t batch_time_ms,
            const string &msg_max_bytes, const int max_trans,
            const uint32_t q_buf_max_msgs, const uint32_t q_buf_max_kb,
            const string &b_version_fback, const string &send_max_retry,
            const string &log_cn_close, const string &rdkafka_debug, 
            const uint32_t no_rsp_timeout_ms,
            const uint32_t metadata_refresh_interval_ms,
            EncodedMsgMap *msg_map, CheckPoint *checkpoint);

  // \brief ProcessKfkAck
  // call when recv write kafka response, update checkpoint if response
  // contain
  // kafka offset
  // \param message
  void ProcessKfkAck(RdKafka::Message &message);

  // \brief Exit
  // exit write kafka thread
  inline void Exit(void) { exit_flag_ = true; }

  size_t GetWaitRspPkgsSize(void);

  bool enable_trans_merge_;
  bool quit_when_timeout_;

 protected:
  // \brief Run
  // get transaction from EncodedMsgMap and write to kafka
  void Run(void);

 private:
  // \brief PluckTrans
  // get transaction from EncodedMsgMap
  // \param seq_no
  // transaction seq_no
  // \return SeqTrans point or NULL
  SeqTrans* PluckSeqTrans(const uint64_t seq_no);

  // \brief WriteKafkaPkg
  //
  // \param seq_no
  // \param pkgs
  // \param writed_size
  //
  // \return
  bool WriteKafkaPkgs(const uint64_t seq_no,
                     vector<SeqTrans *> &seq_trans_vec,
                     size_t *writed_size);

  void WriteKafkaPkg(int partition,
                     string key,
                     SeqTrans *seq_trans_vec,
                     size_t *writed_size);

  // \brief Clear
  void Clear(void);

  bool IsWriteMsgNoRspTimeout();

  void UpdateNoRspTimeIfNoMspSend();

 private:
  // kafka broker list
  string brokerlists_;

  // kafka topic name
  string topic_name_;

  // kafka global conf
  RdKafka::Conf *kafka_conf_;

  // kfaka topic global conf
  RdKafka::Conf *topic_conf_;

  // kafka topic
  RdKafka::Topic *topic_;

  // kafka producer
  RdKafka::Producer *producer_;

  // partitions list
  vector<int> partitions_;

  // kafka error callback function
  KfkProducerECb kfk_event_cb_;

  // write kafka response callback function
  KfkProducerDRCb kfk_dr_cb_;

  // exit flag
  volatile bool exit_flag_;

  // waiting transaction seq no
  uint64_t last_trans_seqno_;

  EncodedMsgMap *msg_map_;

  CheckPoint *checkpoint_;

  // pop max transactions number from encoded_msg_map
  int max_seq_trans_num_;

  // max number waiting for write kafka response
  uint32_t max_wait_rsp_pkgs_num_;

  // save there, but not use
  uint32_t q_buf_max_kb_;

  uint32_t no_rsp_timeout_ms_;

  struct timeval last_recv_rsp_time_;

  MySQLGtidMgr gtid_mgr_;

 public:
  int timeout_count_;
  SeqTransCtx *seq_trans_ctx_; 
};

}  // namespace syncer

#endif  // _KAFKA_PRODUCER_H_
