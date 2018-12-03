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

#include "rebuild.h"

#include <unistd.h>
#include <iostream>
#include <vector>
#include <string>

#include <glog/logging.h>
#include <rdp-comm/util.h>
#include <rdp.pb.h>
#include <lz4.h>

#include "syncer_app.h"
#include "rate_limiter.h"
#include "split_package.h"

using std::string;
using rdp_comm::GetLastErrorMsg;
using rdp_comm::GetIntervalMs;

namespace syncer {

enum {
  TOPIC_CHECK_TIMEOUT = 9001,
  TOPIC_CHECK_PROTO_ERROR = 9002,
  TOPIC_CONSUMER_ERROR = 9003,
  TOPIC_UPDATE_CHECKPOINT_ERROR = 9004,
  DECOMPRESS_ERROR = 9005,

  TOPIC_CHECK_FINISHED = 9999,
};

static uint64_t g_epoch = 0;
static uint64_t g_trans_seq_no = 0;
static int32_t g_last_split_flag = 0;
static uint64_t g_split_seq_no = 0;

static string g_brokers;
static string g_topic;
static string g_b_version_fback;
static unsigned int g_partition = 0;

static uint32_t g_fetch_max_msg_bytes = 1048576;

static uint64_t g_rate = 20;

static string g_kafka_debug;

const int64_t LAST_OFFSET_INIT = -10000;
static int64_t g_last_offset = LAST_OFFSET_INIT;
static int64_t g_start_offset = 0;
static int64_t g_stop_offset = 0;

static KafkaConsumer *g_consumer;

static CheckPoint *g_checkpoint;

static char *g_decompress_buf = NULL;
static size_t g_decompress_buf_size = 0;

static rdp::messages::Transaction *g_transaction = NULL;

static int Decompress(const char *src, const size_t src_size, char **dst, size_t *dst_size) {
	int retry_time = 0;
  int decompressed_data_size = 0;
  do {
    decompressed_data_size = LZ4_decompress_safe(src, *dst, src_size, *dst_size);
    if (decompressed_data_size < 0)
      RDP_LOG_ERROR << "A negative result from LZ4_decompress_safe"
                       "indicates a failure trying to compress the data.  See exit code (echo "
                    << decompressed_data_size << ") for value returned.";
    // 可能是解压空间不够，扩大解压buf，重新解压，重试3次
    if (decompressed_data_size == 0) {
      RDP_LOG_WARN
          << "A result of 0 means compression worked, but was stopped because the destination "
             "buffer couldn't hold all the information.";
      *dst = (char *)realloc(*dst, (*dst_size) * 2);
      assert(*dst != NULL);
      (*dst_size) *= 2;
    }
  } while (decompressed_data_size == 0 && retry_time++ < 3);

  if (decompressed_data_size == 0) {
    RDP_LOG_ERROR << "the destination buffer couldn't hold all the information. buffer size:"
                  << *dst_size << ", maybe decompress.max.buf.size too small";
  }

  return decompressed_data_size;
}

static void ConsumerDestroy() {
  if (NULL == g_consumer) {
    return;
  }
  delete g_consumer;
  g_consumer = NULL;
}

static bool ConsumerInit() {
  if (NULL != g_consumer) {
    return true;
  }

  g_consumer = new KafkaConsumer();
  if (NULL == g_consumer) {
    RDP_LOG_ERROR << "New KafkaConsumer Failt: " << GetLastErrorMsg(errno);
    goto err;
  }

  if (!g_consumer->Init(g_brokers, g_topic, g_partition, g_b_version_fback,
                        g_fetch_max_msg_bytes, g_kafka_debug)) {
    RDP_LOG_ERROR << "Init Kafka Consumer, Brokerlist: " << g_brokers
                  << ", Topic: " << g_topic << ", Partition: " << g_partition
                  << ", Fetch Max Msg Bytes: " << g_fetch_max_msg_bytes
                  << ", Broker Version Fallback:" << g_b_version_fback;
    goto err;
  }

  return true;

err:
  ConsumerDestroy();
  return false;
}

static bool CheckStartOffset() {
  int64_t topic_begin_offset = 0;
  int64_t topic_end_offset = 0;

  if (!g_consumer->QueryOffset(&topic_begin_offset, &topic_end_offset)) {
    RDP_LOG_ERROR << "Consumer Get Low And High Offset Failt!";
    return false;
  }

  RDP_LOG_INFO << "Topic: " << g_topic << ", Partition: " << g_partition
               << ", Offset Low: " << topic_begin_offset
               << ", High: " << topic_end_offset << " Start Offset:" << g_start_offset;

  if (g_start_offset < topic_begin_offset && g_start_offset > topic_end_offset) {
    RDP_LOG_ERROR << "Start Offset: " << g_start_offset 
      << " is not in [" << topic_begin_offset << ":" << topic_end_offset << "]";
    return false;
  }

  g_stop_offset = topic_end_offset;

  return true;
}

static rdp::messages::KafkaPkg *PkgKfkToRdp(RdKafka::Message *msg) {

  ::rdp::messages::VMSMessage *vms_msg = NULL;
  ::rdp::messages::KafkaPkg *pkg = NULL;

  vms_msg = new ::rdp::messages::VMSMessage();
  assert(NULL != vms_msg);

  // decode message to VMSMessage
  if (!vms_msg->ParseFromArray(msg->payload(), msg->len())) {
    RDP_LOG_ERROR << "Decode Brokerlist: " << g_brokers
                  << ", Topic: " << g_topic << ", Partition: " << g_partition
                  << ", Offset: " << msg->offset() << " Failt";
    goto err;
  }

  pkg = new ::rdp::messages::KafkaPkg;
  assert(NULL != pkg);

  // decode VMSMessage to KafkaPkg
  if (!pkg->ParseFromString(vms_msg->payload())) {
    RDP_LOG_ERROR << "Decode VMSMessage: " << g_brokers
                  << ", Topic: " << g_topic << ", Partition: " << g_partition
                  << ", Offset: " << msg->offset() << " Failt";
    goto err;
  }

  delete vms_msg;
  vms_msg = NULL;

  return pkg;

err:
  if (vms_msg) {
    delete vms_msg;
    vms_msg = NULL;
  }
  if (pkg) {
    delete pkg;
    pkg = NULL;
  }
  return NULL;
}

static rdp::messages::Transaction *KafkaPkgToTransaction(rdp::messages::KafkaPkg *pkg, rdp::messages::Transaction *trans) {
  assert(pkg != NULL);
  rdp::messages::Transaction *trans_pkg = NULL;

  if (trans == NULL)
    trans = new rdp::messages::Transaction();

  if (pkg->has_flag() && (pkg->flag() & rdp::messages::kKfkPkgCompressData) == rdp::messages::kKfkPkgCompressData) {
    if (pkg->has_source_data_len() && g_decompress_buf_size < pkg->source_data_len()) {
      g_decompress_buf_size = pkg->source_data_len() > g_decompress_buf_size*1.5 ? pkg->source_data_len() : g_decompress_buf_size*1.5; 
      g_decompress_buf = (char *)realloc(g_decompress_buf, g_decompress_buf_size);
      assert(g_decompress_buf != NULL);
    }
    size_t data_size = Decompress(pkg->data().c_str(), pkg->data().length(), &g_decompress_buf, &g_decompress_buf_size);
    if (data_size <= 0) {
      RDP_LOG_ERROR << "Decompress Error";
      return NULL;
    }
    if (!trans->IsInitialized()) {
      if (!trans->ParseFromArray(g_decompress_buf, data_size)) {
        RDP_LOG_ERROR << "ParseFromString failed";
        return NULL;
      }
      return trans;
    } else {
      trans_pkg = new rdp::messages::Transaction();
      if (!trans_pkg->ParseFromArray(g_decompress_buf, data_size)) {
        RDP_LOG_ERROR << "ParseFromString failed";
        delete trans_pkg;
        return NULL;
      }
    }
  } else {
    if (!trans->IsInitialized()) {
      if (!trans->ParseFromString(pkg->data())) {
        RDP_LOG_ERROR << "ParseFromString failed";
        return NULL;
      }
      return trans;
    } else {
      trans_pkg = new rdp::messages::Transaction();
      if (!trans_pkg->ParseFromString(pkg->data())) {
        RDP_LOG_ERROR << "ParseFromString failed";
        delete trans_pkg;
        return NULL;
      }
    }
  }
  assert(trans_pkg != NULL);
  assert(trans->IsInitialized() == true);
  assert(trans->gtid() == trans_pkg->gtid());
  assert(trans->seq() == trans_pkg->seq());

  RDP_LOG_DBG << "before transaction seq:" << trans->seq() 
              << " events size:" << trans->events_size() 
              << " add transaction seq:" << trans_pkg->seq() 
              << " events size:" << trans_pkg->events_size();

  trans->mutable_events()->MergeFrom(trans_pkg->events());

  RDP_LOG_DBG << "after transaction seq:" << trans->seq() 
              << " events size:" << trans->events_size();

  delete trans_pkg;
   
  return trans;
}

static void SkipPkg(int64_t offset, rdp::messages::KafkaPkg *pkg_rdp) {
  if (!pkg_rdp) abort();

  RDP_LOG_WARN << "( g_epoch:"          << g_epoch 
  //std::cerr    << "( g_epoch:"          << g_epoch 
               << ", g_last_offset:"    << g_last_offset
               << ", g_trans_seq_no: "  << g_trans_seq_no
               << ", g_split_flag: "    << g_last_split_flag
               << ", g_split_seq_no: "  << g_split_seq_no 
               << ")"
               << ", current package will be skipped, which is:"
               << "( offset: "       << offset
               << ", epoch: "        << pkg_rdp->epoch() 
               << ", trans_seq_no: " << pkg_rdp->trans_seq_no()
               << ", split_flag: "   << pkg_rdp->split_flag()
               << ", seq_No: "       << pkg_rdp->seq_no()
               << ", gtid: "         << pkg_rdp->gtid()
               << ") is skiped" ;
  //std::cerr << std::endl;
  return;
}

static bool UpdateCheckPoint(uint64_t epoch, const string &gtid, uint64_t trx_seq_no, uint64_t offset) {
  assert(g_checkpoint != NULL);
  if (!g_checkpoint->AddGtid(gtid)) {
    RDP_LOG_ERROR << "Add GTID: " << gtid << " To CheckPoint GTID Set Failt";
    return false;
  }
  g_checkpoint->SetEpoch(epoch);
  g_checkpoint->SetTransSeqNo(trx_seq_no);
  g_checkpoint->UpdateOffset(offset);
  return true;
}

static int32_t ProcPkg(void) {
  // consume read message return code
  int consume_ret = 0;
  // consume read message return error string
  string consume_err;
  int64_t cur_offset = g_last_offset;
  RdKafka::Message *pkg_kfk = NULL;

  struct timeval curr_time;
  struct timeval last_success;
  gettimeofday(&last_success, NULL);

  // rate limit initialize
  static RateLimiter rate_limiter(g_rate * 1000);

  rdp::messages::KafkaPkg *pkg_rdp = NULL;

  while (cur_offset < g_stop_offset - 1) {

    if (pkg_rdp) {delete pkg_rdp; pkg_rdp = NULL;}
    if (pkg_kfk) {delete pkg_kfk; pkg_kfk = NULL;}

    pkg_kfk = g_consumer->Read(&consume_ret, &consume_err);
    if (consume_ret == KafkaConsumer::CONSUMER_PARTITION_EOF) return TOPIC_CHECK_FINISHED;
    else if (consume_ret == KafkaConsumer::CONSUMER_TIMED_OUT) {
      gettimeofday(&curr_time, NULL);
      if (GetIntervalMs(&last_success, &curr_time) >= 10 * 1000) {
        RDP_LOG_INFO << "Consumer Timeout > 10 Second, Exit";
        return TOPIC_CHECK_TIMEOUT;
      }
      continue;
    } else if (consume_ret != KafkaConsumer::CONSUMER_NO_ERROR) {
      RDP_LOG_ERROR << "Kafka consumer failed, last offset:" <<  cur_offset
                    << ", error code:" << consume_ret << ", error message:"<< consume_err; 
      return TOPIC_CONSUMER_ERROR;
    } 

    gettimeofday(&last_success, NULL);

    rate_limiter.Wait(pkg_kfk->len());

    cur_offset = pkg_kfk->offset(); 

    RDP_LOG_DBG << "Consumer Offset: " << cur_offset;

    if (g_last_offset == LAST_OFFSET_INIT) g_last_offset = cur_offset; 

    pkg_rdp = PkgKfkToRdp(pkg_kfk);

    // epoch       : dynasty for rdp
    // trans_seq_no: transaction sequence in epoch
    // seq_no      : split sequence in transaction
    // split_flag  : 0-> 独立数据，没有分包操作; 1-> 分包数据，非最后分包; 2-> 分包数据，最后分包
    if (pkg_rdp->epoch() < g_epoch) {SkipPkg(cur_offset, pkg_rdp); continue;}  
    if (pkg_rdp->epoch() == g_epoch) {
      if (pkg_rdp->trans_seq_no() < g_trans_seq_no) {SkipPkg(cur_offset, pkg_rdp); continue;}
      if (pkg_rdp->trans_seq_no() == g_trans_seq_no) { 
        if (pkg_rdp->split_flag() == PKG_NOT_SPLIT) {SkipPkg(cur_offset, pkg_rdp); continue;}
        if (pkg_rdp->seq_no() != (g_split_seq_no + 1)) {SkipPkg(cur_offset, pkg_rdp); continue;} //split pkg, but is duplicate
        //Normal pkg is a split package after the first , with incremental seq_no
        //data.append(pkg_rdp->data());
        if (NULL == (g_transaction = KafkaPkgToTransaction(pkg_rdp, g_transaction))) {
          RDP_LOG_ERROR << "Kafka Pkg to transaction error";
          if (g_transaction) delete g_transaction;
          g_transaction = NULL;
          return TOPIC_CHECK_PROTO_ERROR;  
        }
      } else { 
        //if (data.size() > 0) data.clear(); // clear the pre-data
        if (g_transaction != NULL) {
          delete g_transaction;
          g_transaction = NULL;
        }
        // Normal pkg, maybe it is a first split package
        if (pkg_rdp->seq_no() != 0) {SkipPkg(cur_offset, pkg_rdp); continue;} //split pkg, but is duplicate
        // 0.8 版本kafka g_trans_seq_no == 0表示第一个consumer的transaction，不需要校验连续性
        if (g_trans_seq_no != 0 && pkg_rdp->trans_seq_no() != (g_trans_seq_no + 1)) {SkipPkg(cur_offset, pkg_rdp); continue;} //split pkg, but is duplicate
        //data.append(pkg_rdp->data());
        if (NULL == (g_transaction = KafkaPkgToTransaction(pkg_rdp, g_transaction))) {
          RDP_LOG_ERROR << "Kafka Pkg to transaction error";
          if (g_transaction) delete g_transaction;
          g_transaction = NULL;
          return TOPIC_CHECK_PROTO_ERROR;  
        }
      }
    } else {
      //if (data.size() > 0) data.clear(); // clear the pre-data
      if (g_transaction != NULL) {
        delete g_transaction;
        g_transaction = NULL;
      }
      // Normal pkg, maybe it is a first split package
      if (pkg_rdp->seq_no() != 0) {SkipPkg(cur_offset, pkg_rdp); continue;} //split pkg, but is duplicate
      // 0.8 版本kafka g_trans_seq_no == 0表示第一个consumer的transaction，不需要校验连续性
      if (g_trans_seq_no != 0 && pkg_rdp->trans_seq_no() != (g_trans_seq_no + 1)) {SkipPkg(cur_offset, pkg_rdp); continue;} //split pkg, but is duplicate
      //data.append(pkg_rdp->data());
      if (NULL == (g_transaction = KafkaPkgToTransaction(pkg_rdp, g_transaction))) {
        RDP_LOG_ERROR << "Kafka Pkg to transaction error";
        return TOPIC_CHECK_PROTO_ERROR;  
      }
    }

    g_epoch           = pkg_rdp->epoch();
    g_trans_seq_no    = pkg_rdp->trans_seq_no();
    g_split_seq_no    = pkg_rdp->seq_no();
    g_last_split_flag = pkg_rdp->split_flag();

    if (pkg_rdp->split_flag() != 1) {
      break;
    }
  }

  if (pkg_kfk) {delete pkg_kfk; pkg_kfk = NULL;}

  if (g_last_split_flag == 1) {
    RDP_LOG_INFO << "offset[" << g_last_offset << ","<< cur_offset << ") which is part of transaction, is skipped";

    delete g_transaction;
    g_transaction = NULL;

    return TOPIC_CHECK_FINISHED;
  }

  //if (data.empty()) return TOPIC_CHECK_FINISHED; // finished
  if (g_transaction == NULL) return TOPIC_CHECK_FINISHED;
  
  g_last_offset = cur_offset;

  RDP_LOG_DBG << "Found Transaction GTID: " << g_transaction->gtid() 
    << " Epoch:" << g_epoch << " Seq no:" << g_trans_seq_no << " Offset[" << g_last_offset << ":" << cur_offset << "]";

  if (!UpdateCheckPoint(g_epoch, g_transaction->gtid(), g_trans_seq_no, cur_offset)) {
    RDP_LOG_ERROR << "Update Checkpoint GTID: " << g_transaction->gtid() 
      << " Epoch:" << g_epoch << " Seq no:" << g_trans_seq_no << " Offset:" << cur_offset << " Failt";  

    delete g_transaction;
    g_transaction = NULL;

    return TOPIC_UPDATE_CHECKPOINT_ERROR;
  }

  delete g_transaction;
  g_transaction = NULL;

  return 0;
}

static bool Process() {
  int32_t ret = 0;

  // initialize kafka consumer
  if (!ConsumerInit()) {
    RDP_LOG_ERROR << "Initialize Kafka Consumer Failt";
    goto err;
  }

  // revise g_start_offset and g_stop_offset use kafka topic low and high offset
  if (!CheckStartOffset()) {
    RDP_LOG_ERROR << "Check Kafka Consumer Start Offset Failt";
    goto err;
  }

  // start consume kafka message
  if (!g_consumer->Start(g_start_offset)) {
    RDP_LOG_ERROR << "Start Consumer At Offset: " << g_start_offset << " Failt";
    goto err;
  }

  do {
    ret = ProcPkg();
    if (ret) {
      //vms消息消费完了，正常结束
      if (ret == TOPIC_CHECK_FINISHED) break; 

      //错误结束
      goto err;
    }
  } while(true);

  ConsumerDestroy();

  if (g_transaction != NULL) {
    delete g_transaction;
    g_transaction = NULL;
  }

  return 0 == ret || TOPIC_CHECK_FINISHED == ret;

err:

  if (g_transaction != NULL) {
    delete g_transaction;
    g_transaction = NULL;
  }

  abort();

  ConsumerDestroy();

  return false;
}


bool Rebuild(CheckPoint *checkpoint) {
  bool rs = true;

  AppConfig *app_conf = g_syncer_app->app_conf_;

  g_brokers = app_conf->GetValue<string>(string("kafka.brokerlist"));
  g_topic = app_conf->GetValue<string>(string("kafka.topic"));
  g_partition = app_conf->GetValue<uint32_t>(string("kafka.partition"));
  g_fetch_max_msg_bytes =
      app_conf->GetValue<uint32_t>(string("kafka.consumer.fetch_max_msg_bytes"));
  g_b_version_fback = app_conf->GetValue<string>("kafka.b_version_fback");
  g_kafka_debug = app_conf->GetValue<string>("kafka.debug", "");
  g_rate = g_syncer_app->app_conf_->GetValue<size_t>("kafka.comsumer.rate.limit.mb");

  g_decompress_buf_size = g_syncer_app->app_conf_->GetValue<size_t>("decompress.max.buf.size", 10 * 1024 * 1024);
  g_decompress_buf = (char *)malloc(g_decompress_buf_size * sizeof(char));
  assert(g_decompress_buf != NULL);

  g_checkpoint = checkpoint;

  if (g_b_version_fback.compare(0, 3, "0.8") == 0) {
    uint32_t batch_num_msg = app_conf->GetValue<uint32_t>("kafka.producer.batch_num_msg");
    // 0.8版本kafka回退batch message size
    g_start_offset = checkpoint->GetOffset() - batch_num_msg;
    if (g_start_offset < 0) 
      g_start_offset = 0;
  } else {
    // checkpoint记录offset为消息存储的当前位置，所以consumer开始消费位置需要+1
    g_start_offset = checkpoint->GetOffset() + 1;
    g_epoch =  checkpoint->GetEpoch();
    g_trans_seq_no = checkpoint->GetTransSeqNo();
  }

  if (!(rs = Process())) {
    RDP_LOG_ERROR << "Rebuild Process Failt";
  }
  
  free(g_decompress_buf);

  return rs;
}

} // namespace syncer
