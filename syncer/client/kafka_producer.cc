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

#include "kafka_producer.h"
#include "split_package.h"

#include <boost/functional/hash.hpp>
#include <boost/bind.hpp>

#include <rdp-comm/fiu_engine.h>

#include <glog/logging.h>
#include "syncer_app.h"
#include "rebuild.h"

namespace syncer {

static KafkaProducer *g_kafka_producer;

// Kafka producer thread cleanup handler
static void ThreadCleanupHandler(void *args) {}

// if kafka partitions is more then 1
// caculate key hash and find the partition
static int GetPartition(const std::string &key, const vector<int> &partitions) {
  // default partition 0
  if (partitions.empty()) {
    return 0;
  }

  size_t hash_num = boost::hash<std::string>()(key);
  return partitions[hash_num % partitions.size()];
}

void KfkProducerDRCb::dr_cb(RdKafka::Message &message) {
  assert(NULL != g_kafka_producer);
  g_kafka_producer->ProcessKfkAck(message);
}

void KfkProducerECb::event_cb(RdKafka::Event &event) {
  switch (event.type()) {
    case RdKafka::Event::EVENT_ERROR:
      RDP_LOG_ERROR << "Kafka Failt(" << event.err()
                    << "): " << RdKafka::err2str(event.err()) << " "
                    << event.str();

      if (event.err() == RdKafka::ERR__MSG_TIMED_OUT && !g_kafka_producer->quit_when_timeout_) {
        // 如果是超时, 而且没有配置为“超时退出”，那么不立马退出进程，而是当超时超过5次才退出
        if (__sync_fetch_and_add(&rdp_comm::Singleton<KafkaProducer>::GetInstance().timeout_count_, 1) <= 5) break;
      }

      static char buf[1024] = {0};
      snprintf(buf, sizeof(buf) - 1, "ErrCode:(%d)Msg:[%s %s]", event.err(),
               RdKafka::err2str(event.err()).c_str(), event.str().c_str());
      g_syncer_app->Alarm(buf);

      // broker连接断开或者Connection refused 需要忽略, 无需退出
      if (event.err() == RdKafka::ERR__TRANSPORT) {
        RDP_LOG_WARN << "RDP is not going to quit";
        break;
      }

      // 除了ERR__TRANSPORT之外的错误都退出，包括：
      //  ERR__MSG_TIMED_OUT,  ERR__ALL_BROKERS_DOWN
      abort();

    case RdKafka::Event::EVENT_STATS:
      // kafka state message if open
      RDP_LOG_DBG << "Kafka Stats(" << event.err() << "): " << event.str();
      break;

    case RdKafka::Event::EVENT_LOG:
      RDP_LOG_DBG << "Kafka LOG-" << event.severity() << "-" << event.fac() << ":"
                << event.str();
      break;

    case RdKafka::Event::EVENT_THROTTLE:
      // kafka throttle message
      RDP_LOG_ERROR << "Kafka Throttled: " << event.throttle_time()
                 << "ms By: " << event.broker_name() << ", Id "
                 << (int)event.broker_id();
      break;

    default:
      // unknow kafka error, exit producer
      RDP_LOG_ERROR << "Kafka Event(" << event.err() << "): " << event.type()
                    << ", " << RdKafka::err2str(event.err()) << ": "
                    << event.str();
      // if kafka error, exit
      exit(-1);
  }
}

KafkaProducer::KafkaProducer() {
  kafka_conf_ = NULL;
  topic_conf_ = NULL;
  topic_ = NULL;
  producer_ = NULL;
  exit_flag_ = false;
  last_trans_seqno_ = 0;
  enable_trans_merge_ = false;
  quit_when_timeout_ = false;

  __sync_val_compare_and_swap(&timeout_count_, timeout_count_, 0);
  seq_trans_ctx_ = NULL;
}

KafkaProducer::~KafkaProducer() { 
  Clear(); 
}

bool KafkaProducer::Init(const string &brokerlists, const string &topic_name,
                         const vector<int> &partitions,
                         const string &required_acks,
                         const uint32_t batch_num_msg,
                         const uint32_t batch_time_ms,
                         const string &msg_max_bytes, const int max_trans,
                         const uint32_t q_buf_max_msgs,
                         const uint32_t q_buf_max_kb,
                         const string &b_version_fback, 
                         const string &send_max_retry, 
                         const string &log_cn_close, 
                         const string &rdkafka_debug,
                         const uint32_t no_rsp_timeout_ms,
                         const uint32_t metadata_refresh_interval_ms,
                         EncodedMsgMap *msg_map,
                         CheckPoint *checkpoint) {
  assert(NULL != msg_map && NULL != checkpoint);

  bool enable_checksum = g_syncer_app->app_conf_->GetValue<string>("checksum.enable", "0").compare("1") == 0 ? true : false;
  bool enable_compress = g_syncer_app->app_conf_->GetValue<string>("compress.enable", "0").compare("1") == 0 ? true : false;
  size_t split_msg_bytes = g_syncer_app->app_conf_->GetValue<size_t>("kafka.producer.split_msg_bytes");
  bool enable_split_msg = g_syncer_app->app_conf_->GetValue<uint32_t>("kafka.producer.enable_split_msg", 0) == 1 ? true : false;
  bool enable_trans_clean = g_syncer_app->app_conf_->GetValue<uint32_t>("kafka.producer.enable_trans_clean", 0) == 1 ? true : false;
  enable_trans_merge_ = g_syncer_app->app_conf_->GetValue<uint32_t>("kafka.producer.enable_trans_merge", 0) == 1 ? true : false;
  quit_when_timeout_ = g_syncer_app->app_conf_->GetValue<uint32_t>("kafka.producer.quit_when_timeout", 0) == 1 ? true : false;

  seq_trans_ctx_ = new SeqTransCtx(split_msg_bytes,
                                   enable_checksum,
                                   enable_compress,
                                   enable_split_msg,
                                   enable_trans_clean,
                                   2 * 1024 * 1024);
  assert(NULL != seq_trans_ctx_);

  string kafka_err_str;
  char batch_num_msg_str[32] = {0x0};
  size_t batch_num_msg_str_len = 0;
  char batch_time_ms_str[32] = {0x0};
  size_t batch_time_ms_str_len = 0;
  char q_buf_max_msgs_str[32] = {0x0};
  size_t q_buf_max_msgs_str_len = 0;
  char q_buf_max_kb_str[32] = {0x0};
  size_t q_buf_max_kb_str_len = 0;
  char metadata_refresh_interval_ms_str[32] = {0x0};

  this->brokerlists_ = brokerlists;
  this->topic_name_ = topic_name;
  this->msg_map_ = msg_map;
  this->checkpoint_ = checkpoint;
  this->max_seq_trans_num_ = max_trans;
  this->max_wait_rsp_pkgs_num_ = q_buf_max_msgs;
  this->q_buf_max_kb_ = q_buf_max_kb;
  this->no_rsp_timeout_ms_ = no_rsp_timeout_ms;
  g_kafka_producer = &rdp_comm::Singleton<KafkaProducer>::GetInstance();

  // wait transaction seq no is Kafka Transaction Seq no + 1
  last_trans_seqno_ = checkpoint->GetTransSeqNo() + 1;


  // Copy kafka partitions
  for (vector<int>::const_iterator it = partitions.begin();
       it != partitions.end(); ++it) {
    this->partitions_.push_back(*it);
  }

  // Check partitions param
  if (this->partitions_.empty()) {
    RDP_LOG_ERROR << "Kafka Producer Partitions Is Empty, Failt";
    return false;
  }

  // Check acks param
  if (required_acks.compare("0") == 0) {
    RDP_LOG_ERROR << "Kafka kafka.producer.acks Can`t Set '0'";
    return false;
  }

  // Create kafka configure objects, delete in destructor function
  kafka_conf_ = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
  topic_conf_ = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);

  // Set kafka configure brokerlist
  if (RdKafka::Conf::CONF_OK !=
      kafka_conf_->set("metadata.broker.list", brokerlists_, kafka_err_str)) {
    RDP_LOG_ERROR << "Set Rdkafka Configure 'metadata.broker.list': "
               << brokerlists_ << ", Error: " << kafka_err_str;
    goto err;
  }

  // Set kafka configure batch.num.messages
  batch_num_msg_str_len =
      snprintf(batch_num_msg_str, sizeof(batch_num_msg_str) - 1, "%" PRIu32,
               batch_num_msg);
  if (RdKafka::Conf::CONF_OK !=
      kafka_conf_->set("batch.num.messages",
                       string(batch_num_msg_str, batch_num_msg_str_len),
                       kafka_err_str)) {
    RDP_LOG_ERROR << "Set Rdkafka Configure 'batch.num.messages': "
               << string(batch_num_msg_str, batch_num_msg_str_len)
               << ", Error: " << kafka_err_str;
    goto err;
  }

  // Set kafka configure queue.buffering.max.ms
  batch_time_ms_str_len =
      snprintf(batch_time_ms_str, sizeof(batch_time_ms_str) - 1, "%" PRIu32,
               batch_time_ms);
  if (RdKafka::Conf::CONF_OK !=
      kafka_conf_->set("queue.buffering.max.ms",
                       string(batch_time_ms_str, batch_time_ms_str_len),
                       kafka_err_str)) {
    RDP_LOG_ERROR << "Set Rdkafka Configure 'queue.buffering.max.ms': "
               << string(batch_time_ms_str, batch_time_ms_str_len)
               << ", Error: " << kafka_err_str;
    goto err;
  }

  // Set kafka queue.buffering.max.messages
  q_buf_max_msgs_str_len =
      snprintf(q_buf_max_msgs_str, sizeof(q_buf_max_msgs_str) - 1, "%" PRIu32,
               q_buf_max_msgs);
  if (RdKafka::Conf::CONF_OK !=
      kafka_conf_->set("queue.buffering.max.messages",
                       string(q_buf_max_msgs_str, q_buf_max_msgs_str_len),
                       kafka_err_str)) {
    RDP_LOG_ERROR << "Set Rdkafka Configure 'queue.buffering.max.messages': "
               << string(q_buf_max_msgs_str, q_buf_max_msgs_str_len)
               << ", Error: " << kafka_err_str;
    goto err;
  }

  // Set kafka queue.buffering.max.kbytes
  q_buf_max_kb_str_len = snprintf(
      q_buf_max_kb_str, sizeof(q_buf_max_kb_str) - 1, "%" PRIu32, q_buf_max_kb);
  if (RdKafka::Conf::CONF_OK !=
      kafka_conf_->set("queue.buffering.max.kbytes",
                       string(q_buf_max_kb_str, q_buf_max_kb_str_len),
                       kafka_err_str)) {
    RDP_LOG_ERROR << "Set Rdkafka Configure 'queue.buffering.max.kbytes': "
               << string(q_buf_max_kb_str, q_buf_max_kb_str_len)
               << ", Error: " << kafka_err_str;
    goto err;
  }

  // Set kafka configure message.max.bytes
  if (RdKafka::Conf::CONF_OK !=
      kafka_conf_->set("message.max.bytes", msg_max_bytes, kafka_err_str)) {
    RDP_LOG_ERROR << "Set Rdkafka Configure 'message.max.bytes': " << msg_max_bytes
               << ", Error: " << kafka_err_str;
    goto err;
  }

  // Set kafka broker.version.fallback
  if (RdKafka::Conf::CONF_OK != kafka_conf_->set("broker.version.fallback",
                                                 b_version_fback,
                                                 kafka_err_str)) {
    RDP_LOG_ERROR << "Set Rdkafka Configure 'broker.version.fallback': "
               << b_version_fback << ", Error: " << kafka_err_str;
    goto err;
  }


  // Set kafka log.connection.close
  if (RdKafka::Conf::CONF_OK != kafka_conf_->set("log.connection.close",
                                                 log_cn_close,
                                                 kafka_err_str)) {
    RDP_LOG_ERROR << "Set Rdkafka Configure 'log.connection.close': "
               << log_cn_close << ", Error: " << kafka_err_str;
    goto err;
  }

  // Set kafka debug
  // default is ""
  if (!rdkafka_debug.empty() &&
      RdKafka::Conf::CONF_OK != kafka_conf_->set("debug",
                                                 rdkafka_debug, kafka_err_str)) {
    RDP_LOG_ERROR << "Set Rdkafka Configure 'debug': "
               << rdkafka_debug << ", Error: " << kafka_err_str;
    goto err;
  }

  // Set kafka configure event_callback
  if (RdKafka::Conf::CONF_OK !=
      kafka_conf_->set("event_cb", &kfk_event_cb_, kafka_err_str)) {
    RDP_LOG_ERROR
        << "Set Rdkafka Configure 'event_cb': RdpKfkProducerEventCb, Error: "
        << kafka_err_str;
    goto err;
  }

  /* Set kafka configure delivery report callback */
  if (RdKafka::Conf::CONF_OK !=
      kafka_conf_->set("dr_cb", &kfk_dr_cb_, kafka_err_str)) {
    RDP_LOG_ERROR << "Ser Rdkafka Configure 'dr_cb': "
                  "RdpKfkProducerDeliveryReportCb  , Error: " << kafka_err_str;
    goto err;
  }

  // Set kafka message.send.max.retries
  if (RdKafka::Conf::CONF_OK != kafka_conf_->set("message.send.max.retries",
                                                 send_max_retry,
                                                 kafka_err_str)) {
    RDP_LOG_ERROR << "Set Rdkafka Configure 'message.send.max.retries': "
               << send_max_retry << ", Error: " << kafka_err_str;
    goto err;
  }

  if (RdKafka::Conf::CONF_OK !=
      topic_conf_->set("request.required.acks", required_acks, kafka_err_str)) {
    RDP_LOG_ERROR << "Set RdKafka Configure 'request.required.acks': "
               << required_acks << ", Error: " << kafka_err_str;
    goto err;
  }

  if (RdKafka::Conf::CONF_OK !=
      topic_conf_->set("produce.offset.report", "true", kafka_err_str)) {
    RDP_LOG_ERROR << "Set RdKafka Configure 'produce.offset.report': true Error: " << kafka_err_str;
    goto err;
  }

  // Set kafka topic.metadata.refresh.interval.ms
  snprintf(metadata_refresh_interval_ms_str, sizeof(metadata_refresh_interval_ms_str) - 1, "%" PRIu32, metadata_refresh_interval_ms);
  if (RdKafka::Conf::CONF_OK != kafka_conf_->set("topic.metadata.refresh.interval.ms",
                                                 metadata_refresh_interval_ms_str, 
                                                 kafka_err_str)) {
    RDP_LOG_ERROR << "Set Rdkafka Configure 'topic.metadata.refresh.interval.ms': "
               << metadata_refresh_interval_ms << ", Error: " << kafka_err_str;
    goto err;
  }

  // Create kafka producer
  producer_ = RdKafka::Producer::create(kafka_conf_, kafka_err_str);
  if (!producer_) {
    RDP_LOG_ERROR << "Create Kafka Producer Failt: " << kafka_err_str;
    goto err;
  }

  // Create kafka topic
  topic_ = RdKafka::Topic::create(producer_, topic_name_, topic_conf_,
                                  kafka_err_str);
  if (!topic_) {
    RDP_LOG_ERROR << "Create Kafka Topic: " << topic_name_
               << ", Failt: " << kafka_err_str;
    goto err;
  }

  RDP_LOG_DBG << "Create Kafka Producer: " << producer_->name()
            << ", Topic: " << topic_->name() << ", Success";

  return true;

err:
  Clear();

  return false;
}

void KafkaProducer::Run(void) {
  RDP_LOG_DBG << "Start Kafka Producer";

  uint64_t stats_pkg_num = 0;
  size_t stats_pkg_len = 0;
  size_t writed_len = 0;

  struct timeval curr_time;
  struct timeval pre_time;

  PthreadCall("pthread_detach", pthread_detach(pthread_self()));
  pthread_cleanup_push(ThreadCleanupHandler, this);

  rdp_comm::signalhandler::AddThread(pthread_self(), "producer.thd");

  vector<SeqTrans *> seq_trans_vec;

  // 启动时初始化last_recv_rsp_time_
  gettimeofday(&last_recv_rsp_time_, NULL);

  while (!exit_flag_ && rdp_comm::signalhandler::IsRunning()) {
    gettimeofday(&curr_time, NULL);

    /*
    * This should typically be done prior to destroying a producer instance to
    * make sure all queued and in-flight produce requests are completed before
    * terminating.
    */
#ifndef NO_KAFKA
    // use poll can noblock at least 1ms
    // if no pkg wait to send, block in PopMsg
    producer_->poll(0);
#endif
    
    // 判断last_recv_rsp_time_是否超时
    //if (IsWriteMsgNoRspTimeout()) {
    //  abort();
    //}

    // get transaction
    msg_map_->PopMsg(last_trans_seqno_, seq_trans_vec, max_seq_trans_num_);
    if (seq_trans_vec.size() == 0) {
      RDP_LOG_DBG << "Kafka Producer Wait For:" << last_trans_seqno_;
      // 如果out_len()==0，更新last_recv_rsp_time_
      UpdateNoRspTimeIfNoMspSend();
      // if no response, it will block 1ms
      // block 1ms reduce the utilization of cpu
      producer_->flush(1);

      continue;
    }

    int seq_trans_vec_pkgs_num = 0;
    for (vector<SeqTrans *>::iterator it = seq_trans_vec.begin();
         it != seq_trans_vec.end(); ++it) {
      seq_trans_vec_pkgs_num += (*it)->pkgs.size();
    }

#ifdef STAT_TIME
    for (vector<SeqTrans *>::iterator it = seq_trans_vec.begin();
         it != seq_trans_vec.end(); ++it) {
      STAT_TIME_POINT(STAT_T12, (*it)->trans_seq_no, (*it)->stat_point);
#ifdef NO_KAFKA
      STAT_TIME_POINT_END((*it)->stat_point);
#endif
    }
#endif

#ifndef NO_KAFKA
    if (!WriteKafkaPkgs(last_trans_seqno_, seq_trans_vec, &writed_len)) {
      RDP_LOG_ERROR << "Write Transactions To Kafka Failt";
      // write kafka error, exit
      exit(-1);
    }
#endif

    // total sent kafka transaction metric
    g_syncer_metric->total_sent_trx_count_metric_->AddValue(seq_trans_vec.size());
    // total sent kafka message metric
    g_syncer_metric->total_sent_msg_count_metric_->AddValue(seq_trans_vec_pkgs_num);
    // Expose the number of trx to reporter
    g_syncer_metric->producer_trans_tps_metric_->AddValue(seq_trans_vec.size());
    g_syncer_metric->producer_msg_tps_metric_->AddValue(seq_trans_vec_pkgs_num);
    g_syncer_metric->out_rate_metric_->AddValue(writed_len);


    stats_pkg_num += seq_trans_vec_pkgs_num;
    stats_pkg_len += writed_len;

    // add transaction seq_no
    last_trans_seqno_ += seq_trans_vec.size();

    if (10000 <= GetIntervalMs(&pre_time, &curr_time)) {
      RDP_LOG_DBG << "Kafka Producer Process Transaction Numbers: "
                << stats_pkg_num << " Data Size: " << stats_pkg_len
                << ", Producer Out Len: " << producer_->outq_len()
                << ", Wait Kafka Response Transaction Number: "
                << producer_->outq_len();
      pre_time.tv_sec = curr_time.tv_sec;
      pre_time.tv_usec = curr_time.tv_usec;
      stats_pkg_num = 0;
      stats_pkg_len = 0;
    }

#ifdef NO_KAFKA
    for (uint64_t i = 0; i < seq_trans_vec.size(); i++) {
      for (vector<struct SeqKafkaPkg *>::iterator it =
               seq_trans_vec[i]->pkgs.begin();
           it != seq_trans_vec[i]->pkgs.end(); ++it) {
        delete (*it);
      }
      seq_trans_vec[i]->pkgs.clear();
      delete seq_trans_vec[i];
    }
#endif
    seq_trans_vec.clear();
  }

  // Quit parsing from signal thread
  rdp_comm::signalhandler::Wait();
  pthread_cleanup_pop(1);

  RDP_LOG_DBG << "Kafka Producer Stop";
}

bool KafkaProducer::WriteKafkaPkgs(const uint64_t seq_no,
                                  vector<SeqTrans *> &seq_trans_vec,
                                  size_t *writed_size) {
  int partition = 0;
  string key;
  *writed_size = 0;
  SeqTrans *seq_trans = NULL;
  rdp::messages::Transaction transaction;
  uint64_t transaction_seq_no = 0;

  RDP_LOG_DBG << "current batch size:" << seq_trans_vec.size();
  vector<SeqTrans *>::iterator iter = seq_trans_vec.begin();
  for (; iter != seq_trans_vec.end(); ++iter) {
    seq_trans = (*iter);
    assert(NULL != seq_trans);

    key = seq_trans->gtid;

    // get kafka partition
    if (partitions_.size() > 1) {
      partition = GetPartition(key, partitions_);
    } else {
      partition = partitions_[0];
    }

    if(seq_trans->event_count == 0 && enable_trans_merge_) {
      RDP_LOG_DBG << "New clean transaction, "  << "start:" << seq_trans->transaction.binlog_file_name() << ":" << seq_trans->transaction.position()
                                                << ", end:" << seq_trans->transaction.next_binlog_file_name() << ":" << seq_trans->transaction.next_position();
      if (transaction.IsInitialized()) {
        if (transaction.next_binlog_file_name() == seq_trans->transaction.binlog_file_name() &&
            transaction.next_position()         == seq_trans->transaction.position()) {
          transaction.set_next_binlog_file_name(seq_trans->transaction.next_binlog_file_name());
          transaction.set_next_position(seq_trans->transaction.next_position());

          if (!seq_trans->transaction.gtid().empty() && !gtid_mgr_.AddText(seq_trans->transaction.gtid().c_str())) {
            RDP_LOG_ERROR << "Add Gtid:" << seq_trans->transaction.gtid() << " To Kafka Gtid Mgr:" << gtid_mgr_.ToString() << " Failt";
            abort();
          }
        } else {
          transaction.set_gtid(gtid_mgr_.ToString());
          gtid_mgr_.Reset();

          SeqTrans *tmp_seq_trans = TransactionToSeqTrans(&transaction, seq_trans_ctx_, transaction_seq_no);
          assert(tmp_seq_trans != NULL);
          transaction.Clear();

          WriteKafkaPkg(partition, key, tmp_seq_trans, writed_size);
          transaction = seq_trans->transaction;
          transaction_seq_no = seq_trans->trans_seq_no;

          if (!transaction.gtid().empty() && !gtid_mgr_.AddText(transaction.gtid().c_str())) {
            RDP_LOG_ERROR << "Add Gtid:" << transaction.gtid() << " To Kafka Gtid Mgr:" << gtid_mgr_.ToString() << " Failt";
            abort();
          }
        }
      } else {
        transaction = seq_trans->transaction;
        transaction_seq_no = seq_trans->trans_seq_no;
        gtid_mgr_.Reset(); 

        if (!transaction.gtid().empty() && !gtid_mgr_.AddText(transaction.gtid().c_str())) {
          RDP_LOG_ERROR << "Add Gtid:" << transaction.gtid() << " To Kafka Gtid Mgr:" << gtid_mgr_.ToString() << " Failt";
          abort();
        }
      }

      seq_trans->ClearPkgs();
      delete seq_trans;
      seq_trans = NULL;
      continue;
    }

    if (enable_trans_merge_ && transaction.IsInitialized()) {
      transaction.set_gtid(gtid_mgr_.ToString());
      gtid_mgr_.Reset();

      SeqTrans *tmp_seq_trans = TransactionToSeqTrans(&transaction, seq_trans_ctx_, transaction_seq_no);
      assert(tmp_seq_trans != NULL);
      transaction.Clear();

      WriteKafkaPkg(partition, key, tmp_seq_trans, writed_size);
    }

    WriteKafkaPkg(partition, key, seq_trans, writed_size);
    seq_trans->transaction.Clear();
    seq_trans = NULL;
  }

  if (enable_trans_merge_ && transaction.IsInitialized()) {
    transaction.set_gtid(gtid_mgr_.ToString());
    gtid_mgr_.Reset();

    SeqTrans *tmp_seq_trans = TransactionToSeqTrans(&transaction, seq_trans_ctx_, transaction_seq_no);
    assert(tmp_seq_trans != NULL);
    transaction.Clear();

    WriteKafkaPkg(partition, key, tmp_seq_trans, writed_size);
  }

  // poll in Run function
  return true;
}

void KafkaProducer::WriteKafkaPkg(int partition,
                                  string key,
                                  SeqTrans *seq_trans,
                                  size_t *writed_size) {
  SeqKafkaPkg *seq_pkg = NULL;
  string seq_pkg_key;
  RdKafka::ErrorCode write_response;


  RDP_LOG_DBG << "[Write to kafka],"
              << "transaction, " << "start:" << seq_trans->transaction.binlog_file_name() << ":" << seq_trans->transaction.position()
              << ",end:"   << seq_trans->transaction.next_binlog_file_name() << ":" << seq_trans->transaction.next_position();

  vector<struct SeqKafkaPkg *>::iterator pkg_it = seq_trans->pkgs.begin();
  for (; pkg_it != seq_trans->pkgs.end(); ++pkg_it) {
    seq_pkg = *pkg_it;

    // get transaction->seq_no-kafka_pkg->seq_no as key
    seq_pkg_key = seq_pkg->Key();

#if 0
    char buf_tmp[10240] = {0x0};
    size_t buf_tmp_len = 0;
    buf_tmp_len = snprintf(buf_tmp, sizeof(buf_tmp), "Producer:%d\n", seq_pkg->buffer_size);
    for (size_t i = 0; i < seq_pkg->buffer_size; ++i) {
      buf_tmp_len += snprintf(buf_tmp + buf_tmp_len, sizeof(buf_tmp) - buf_tmp_len, "%d ", (int)(seq_pkg->buffer[i]));
    }
    printf("%s\n", buf_tmp);
#endif

    producer_->poll(0);

    while (1) {
      write_response = producer_->produce(
          topic_, partition,
          RdKafka::Producer::RK_MSG_COPY /* Copy payload RK_MSG_COPY*/,
          seq_pkg->buffer, seq_pkg->buffer_size, &key, seq_pkg);
      // 写成功在外面判断是否超时不回复
      if (write_response == RdKafka::ERR_NO_ERROR) {
        break;
      }

      // 超时直接退出
      //if (IsWriteMsgNoRspTimeout()) {
      //  abort();
      //}

      // Librdkafka out Queue is full, wait response callback, reduce
      // producer->outq_len
      if (write_response == RdKafka::ERR__QUEUE_FULL) {
        RDP_LOG_INFO << "Librdkafka Out Queue Is Full, Out Queue Len: "
          << producer_->outq_len()
          << ", Max Queue Len: " << max_wait_rsp_pkgs_num_
          << ", ErrCode: " << write_response;
        // if no response, it will block 1ms
        // block 1ms reduce the utilization of cpu
        for (int i = 0; i < 1000; i++) producer_->flush(1);
        continue;
      }

      // others Error return
      // ErrCode:ERR_MSG_SIZE_TOO_LARGE,ERR__UNKNOWN_PARTITION,ERR__UNKNOWN_TOPIC
      RDP_LOG_ERROR << "Kafka Produce Write Transaction: " << seq_pkg_key
                    << ", Gtid: " << key << ", Failed(" << write_response
                    << "): " << RdKafka::err2str(write_response);
      string alarm_str;
      alarm_str.append("Write Trx:");
      alarm_str.append(seq_pkg_key);
      alarm_str.append(", Gtid:");
      alarm_str.append(key);
      alarm_str.append(" , ERROR:");
      alarm_str.append(RdKafka::err2str(write_response));

      g_syncer_app->Alarm(alarm_str.c_str());
      // librakafka退出有可能死锁，直接abort
      abort();
    }

    RDP_LOG_DBG << "Kafka Producer Write Transaction SeqNo: " << seq_pkg_key
                << ", Gtid: " << key << " Length: " << seq_pkg->buffer_size
                << ", To Topic: " << topic_name_
                << " Partition: " << partition;

    *writed_size += seq_pkg->buffer_size;

    // write kafka pkg avg consumption time
    gettimeofday(&seq_pkg->sent, NULL);

    seq_pkg = NULL;

    FIU_EXECUTE("not_complete_split_transaction", {
      RDP_LOG_DBG << "not_complete_split_transaction size:" << seq_trans->pkgs.size();
      // 消息已经分包，并且已经写了部分分包
      if (seq_trans->pkgs.size() > 5 && pkg_it >= (seq_trans->pkgs.begin() + seq_trans->pkgs.size() / 2)) {
        // 等待message都已经写入kafka
      while(producer_->outq_len() > 0) {
        producer_->poll(0);  
        sleep(1); 
      }
      RDP_LOG_DBG << "waiting for save checkpoint";
      sleep(20);
      abort();
      }
    });
  }

  return;
}

void KafkaProducer::ProcessKfkAck(RdKafka::Message &message) {

  __sync_val_compare_and_swap(&timeout_count_, timeout_count_, 0);
  SeqKafkaPkg *seq_pkg = static_cast<SeqKafkaPkg *>(message.msg_opaque());
  assert(NULL != seq_pkg);

  string seq_pkg_key = seq_pkg->Key();

  RDP_LOG_DBG << "Kafka Producer Recv Transaction SeqNo: " << seq_pkg_key
             << ", Gtid: " << seq_pkg->trans->gtid << " Response Offset:" << message.offset();

  // recv write kafka Response time
  gettimeofday(&seq_pkg->recv_rsp, NULL);

  // save message offset
  if (message.offset() >= 0) {
    seq_pkg->offset = message.offset();
  } else {
    RDP_LOG_ERROR << "Recv transaction: " << seq_pkg_key
                  << " kafka response offset: " << message.offset() << " < 0";
    abort();
  }

  // 更新最后收到response时间
  gettimeofday(&last_recv_rsp_time_, NULL);

  checkpoint_->PushSeqKafkaPkg(seq_pkg);
}

void KafkaProducer::Clear() {
  if (seq_trans_ctx_) {
    delete seq_trans_ctx_;
    seq_trans_ctx_ = NULL;
  }

  if (kafka_conf_) {
    delete kafka_conf_;
    kafka_conf_ = NULL;
  }

  if (topic_conf_) {
    delete topic_conf_;
    topic_conf_ = NULL;
  }

  if (topic_) {
    delete topic_;
    topic_ = NULL;
  }

  if (producer_) {
    delete producer_;
    producer_ = NULL;
  }

  // Wait Kafka release resource
  RdKafka::wait_destroyed(5000);
}

size_t KafkaProducer::GetWaitRspPkgsSize() {
  return producer_ == NULL ? 0 : producer_->outq_len();
}

bool KafkaProducer::IsWriteMsgNoRspTimeout() {
  // 判断producer的输出buffer是否有message，如果没有则不需要校验
  if (0 == GetWaitRspPkgsSize()) 
    return false;

  // 获取当前时间
  struct timeval now;
  gettimeofday(&now, NULL);

  RDP_LOG_DBG << "no_rsp_timeout_ms:" << no_rsp_timeout_ms_ << " interval time ms:" << GetIntervalMs(&last_recv_rsp_time_, &now);

  // 判断是否超时
  if (no_rsp_timeout_ms_ < GetIntervalMs(&last_recv_rsp_time_, &now)) {
    // 格式化last_recv_rsp_time_
    string last_recv_rsp_time_str = FormatTimeval(&last_recv_rsp_time_);
    string alarm_str;

    if (!last_recv_rsp_time_str.empty()) {
      alarm_str = "wait kfk rsp timeout, last rsp time:" + last_recv_rsp_time_str;

      RDP_LOG_ERROR << alarm_str;
      g_syncer_app->Alarm(alarm_str.c_str());
    } else {
      alarm_str = "wait kfk rsp timeout";

      RDP_LOG_ERROR << alarm_str;
      g_syncer_app->Alarm(alarm_str.c_str());
    }
    return true;
  }
  return false;
}

void KafkaProducer::UpdateNoRspTimeIfNoMspSend() {
  // 如果当前没有写Message到Kafka，并且outq_len()为0，说明当前没有数据写到kafka
  // 需要更新最后接收到response时间
  if (0 == GetWaitRspPkgsSize()) {
    RDP_LOG_DBG << "update last_recv_rsp_time_";
    gettimeofday(&last_recv_rsp_time_, NULL);
  }
}

}  // namespace syncer
