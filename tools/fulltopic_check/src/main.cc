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
#include <unistd.h>

#include <iostream>
#include <vector>
#include <string>

#include <glog/logging.h>
#include <rdp-comm/util.h>
#include <rdp-comm/kafka_consumer.h>

#include <rdp.pb.h>
#include <lz4.h>

#include "continuity_check.h"
#include "rate_limiter.h"
#include "broken_points.h"
#include "logger.h"
#include "write_csv.h"

extern char *optarg;
extern int optind, opterr, optopt;

using std::string;
using syncer::KafkaConsumer;
using syncer::RateLimiter;
using rdp_comm::GetLastErrorMsg;
using rdp_comm::GetIntervalMs;
using syncer::LoggerGoogleImpl;
using syncer::g_logger;

enum {
  TOPIC_CHECK_TIMEOUT = 9001,
  TOPIC_CHECK_PROTO_ERROR = 9002,
  TOPIC_CONSUMER_ERROR = 9003,
  DECOMPRESS_ERROR = 9004,
  TOPIC_CHECK_FINISHED = 9999,
};

typedef struct Msg {
  uint64_t offset;
  ::rdp::messages::KafkaPkg *pkg;
  Msg(uint64_t p_offset, ::rdp::messages::KafkaPkg *p_pkg) {
    offset = p_offset;
    pkg = p_pkg;
  }
} Msg;

typedef std::vector<Msg *> KafkaMsgVector;

enum CheckKafkaPkgRs {
  KAFKA_PKG_IGNORE = 0x01,
  KAFKA_PKG_CLEAR_ADD = 0x02,
  KAFKA_PKG_COMPLETE = 0x04,
  KAFKA_PKG_CLEAR = 0x08,
  KAFKA_PKG_ADD = 0x10,
};

enum PkgSplitFlag {
  PKG_NOT_SPLIT = 0,
  PKG_SPLIT = 1,
  PKG_SPLIT_END = 2
};

const char *instructing_information =
    "Usage: %s [-p<partition>] [-o<start offset> -e<end offset>|-a] "
    "-b<host1:port1,host2:port2,..> -t<topic> "
    "-f<broker.version.fallback> -m<fetch.message.max.bytes>\n "
    "\n"
    "librdkafka version %s (0x%08x)\n"
    "\n"
    " Options:\n"
    "  -p <num>       Use Partitioner:\n"
    "                 default: RdKafka::Topic::PARTITION_UA\n"
    "  -o <offset>    Start Offset \n"
    "  -e <offset>    Stop Offset \n"
    "  -a             check all\n"
    "  -b <brokers>   Broker address (localhost:9092)\n"
    "  -t <topic>     Topic to fetch \n"
    "  -r <MB/second> rate limit MB per second\n"
    "                 default 20MB/s \n"
    "  -w             write transaction to csv, default not output\n"
    "  -d             librdkafka debug model, default false\n"
    "  -a             check topic all data\n"
    "  -f <broker.version.fallback> kafka server version \n"
    "  -m <fetch.message.max.bytes> kafka consumer fetch.message.max.bytes \n"
    "                 default: 1048576(1MB) \n"
    "  -g <log dir>   log dir \n"
    "                 default: ./ \n"
    "  -v <log level> log level \n"
    "                 default: 0 \n"
    "  -j <json file> output json file \n"
    "  -Z <default decompress buffer size> default: 10Mb\n";

const static int64_t OFFSET_INVALID = -999;

static unsigned int g_partition = 0;

static int64_t g_start_offset = OFFSET_INVALID;
static int64_t g_stop_offset = OFFSET_INVALID;

static string g_brokers;
static string g_topic;
static string g_b_version_fback;

static uint32_t g_fetch_max_msg_bytes = 1048576;

static bool g_out_csv = false;

static bool g_check_topic_all_data = false;

static uint64_t g_rate = 20;

static bool g_kfk_debug = false;

const int64_t LAST_OFFSET_INIT = -10000;
static int64_t g_last_offset = LAST_OFFSET_INIT;
static uint64_t g_epoch = 0;
static uint64_t g_trans_seq_no = 0;
static int32_t g_last_split_flag = 0;
static uint64_t g_split_seq_no = 0;

static string g_log_dir = "./";
static int g_log_level = 0;

static string g_json_file = "";

static KafkaConsumer *g_consumer = NULL;

static char *g_decompress_buf = NULL;
static size_t g_decompress_buf_size = 10 * 1240 * 1024;

static rdp::messages::Transaction *g_transaction = NULL;

// init kafka consumer
bool ConsumerInit();
// destroy kafka consumer
void ConsumerDestroy();
// print usage info
void Usage(char **argv);
// opt process
bool ParseOptions(int argc, char **argv);
// check argvs params
bool CheckOptions();
// process kafka msg
bool Process();
// revise g_start_offset and g_stop_offset use kafka topic low and high offset
bool ReviseStartStopOffset();
// parse kafka message to kafka package
rdp::messages::KafkaPkg *PkgKfkToRdp(RdKafka::Message *msg);
// check PackingMsg is Match Serial
bool ProcessPackMsg(PackingMsg *msg);

#define ALLOC_BUF(buf, need_size, buf_size)                   \
  do {                                                        \
    if ((buf) == NULL) {                                      \
      if ((buf_size) < (need_size)) (buf_size) = (need_size); \
      (buf) = (char *)malloc((buf_size));                     \
      assert((buf) != NULL);                                  \
      break;                                                  \
    }                                                         \
    if ((need_size) > (buf_size)) {                           \
      (buf) = (char *)realloc((buf), (need_size));            \
      assert(NULL != (buf));                                  \
      (buf_size) = (need_size);                               \
    }                                                         \
  } while (0);

#define FREE_BUF(buf)    \
  do {                   \
    if ((buf) != NULL) { \
      free(buf);         \
      (buf) = NULL;      \
    }                    \
  } while (0);

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

static rdp::messages::Transaction *KafkaPkgToTransaction(rdp::messages::KafkaPkg *pkg, rdp::messages::Transaction *trans) {
  assert(pkg != NULL);
  rdp::messages::Transaction *trans_pkg = NULL;

  if (trans == NULL)
    trans = new rdp::messages::Transaction();

  if (pkg->has_flag() && (pkg->flag() & rdp::messages::kKfkPkgCompressData) == rdp::messages::kKfkPkgCompressData) {
    if (g_decompress_buf == NULL) {
      g_decompress_buf = (char*)malloc(g_decompress_buf_size * sizeof(char));
      if (g_decompress_buf == NULL) exit(-1);
    }

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


int main(int argc, char **argv) {
  // program options process
  if (!ParseOptions(argc, argv)) {
    Usage(argv);
    exit(-1);
  }
  // check program options
  if (!CheckOptions()) {
    Usage(argv);
    exit(-1);
  }

  // initialize log
  g_logger = new LoggerGoogleImpl(argv[0], g_log_dir, g_log_level);

  RDP_LOG_INFO << "Broker: '" << g_brokers << "'";

  if (Process()) {
    // output broken points to log file
    ErrPrint();
    // write json file
    if (!g_json_file.empty()) {
      if (!ErrToJson(g_json_file, g_start_offset, g_stop_offset)) {
        RDP_LOG_ERROR << "Output Json File Failt";
        goto err;
      }
    }
  } else {
    RDP_LOG_ERROR << "Process Failt";
    goto err;
  }

  if (g_logger) {
    delete g_logger;
  }
  FREE_BUF(g_decompress_buf);
  return 0;

err:
  if (g_logger) {
    delete g_logger;
  }
  FREE_BUF(g_decompress_buf);
  return -1;
}

void Usage(char **argv) {
  fprintf(stderr, instructing_information, argv[0], RdKafka::version_str().c_str(),
          RdKafka::version());
}

bool ParseOptions(int argc, char **argv) {
  int opt = 0;
  while ((opt = getopt(argc, argv, "p:o:b:t:f:m:r:e:g:v:Z:j:wda")) != -1) {
    switch (opt) {
      case 'p':
        g_partition = atoi(optarg);
        break;

      case 'o':
        g_start_offset = atol(optarg);
        break;

      case 'b':
        g_brokers = optarg;
        break;

      case 't':
        g_topic = optarg;
        break;

      case 'f':
        g_b_version_fback = optarg;
        break;

      case 'm':
        g_fetch_max_msg_bytes = atol(optarg);
        break;

      case 'w':
        g_out_csv = true;
        break;

      case 'r':
        g_rate = atol(optarg);
        break;

      case 'd':
        g_kfk_debug = true;
        break;

      case 'e':
        g_stop_offset = atol(optarg);
        break;

      case 'a':
        g_check_topic_all_data = true;
        break;

      case 'g':
        g_log_dir = optarg;
        break;

      case 'v':
        g_log_level = atoi(optarg);
        break;

      case 'j':
        g_json_file = optarg;
        break;

      case 'Z':
        g_decompress_buf_size = strtol(optarg, NULL, 10);
        break;

      default:
        printf("Unknow param:'%c'\n", opt);
        return false;
    }
  }

  if (optind != argc) {
    return false;
  }

  return true;
}

bool CheckOptions() {
  // not set kafka topic name
  if (g_topic.empty()) {
    std::cerr << "-t kafka topic name is empty" << std::endl;
    return false;
  }

  // not set start offset and stop offset and consume messages count
  if (g_start_offset == OFFSET_INVALID && g_stop_offset == OFFSET_INVALID &&
      g_check_topic_all_data == false) {
    std::cerr << "[-o<start ffset> -e<stop offset>|-a] "
                 "is invalib" << std::endl;
    return false;
  }

  if (g_check_topic_all_data &&
      (g_start_offset != OFFSET_INVALID || g_stop_offset != OFFSET_INVALID)) {
    // set consume messages count and start offset or stop offset
    std::cerr << "Can`t set -a and -o/-e" << std::endl;
    return false;
  }
  // start offset is less then 0 and not topic beginning
  if (!g_check_topic_all_data &&
      (g_start_offset < 0 || g_stop_offset < 0 || g_start_offset >= g_stop_offset)) {
    std::cerr << "Start Offset: " << g_start_offset << " Or Stop Offset: " << g_stop_offset
              << ", Is Invalid" << std::endl;
    return false;
  }
  return true;
}

void SkipPkg(int64_t offset, rdp::messages::KafkaPkg *pkg_rdp) {
  if (!pkg_rdp) abort();

  RDP_LOG_WARN << "( g_epoch:" << g_epoch
      // std::cerr    << "( g_epoch:"          << g_epoch
               << ", g_last_offset:" << g_last_offset << ", g_trans_seq_no: " << g_trans_seq_no
               << ", g_split_flag: " << g_last_split_flag << ", g_split_seq_no: " << g_split_seq_no
               << ")"
               << ", current package will be skipped, which is:"
               << "( offset: " << offset << ", epoch: " << pkg_rdp->epoch()
               << ", trans_seq_no: " << pkg_rdp->trans_seq_no()
               << ", split_flag: " << pkg_rdp->split_flag() << ", seq_No: " << pkg_rdp->seq_no()
               << ", gtid: " << pkg_rdp->gtid() << ") is skiped";
  // std::cerr << std::endl;
  return;
}

int32_t GetNextPkg(PackingMsg *packing_msg) {
  // consume read message return code
  int consume_ret = 0;
  // consume read message return error string
  string consume_err;
  // string data;
  int64_t cur_offset = g_last_offset;
  RdKafka::Message *pkg_kfk = NULL;
  rdp::messages::KafkaPkg *pkg_rdp = NULL;
  struct timeval curr_time;
  struct timeval last_success;
  gettimeofday(&last_success, NULL);

  // rate limit initialize
  static RateLimiter rate_limiter(g_rate * 1000);

  if (g_last_offset >= (g_stop_offset - 1)) {
    if (g_transaction) delete g_transaction;
    g_transaction = NULL;
    return TOPIC_CHECK_FINISHED;
  }

  while (cur_offset < g_stop_offset - 1) {

    if (pkg_kfk) {
      delete pkg_kfk;
      pkg_kfk = NULL;
    }

    if (pkg_rdp) {
      delete pkg_rdp;
      pkg_rdp = NULL;
    }

    pkg_kfk = g_consumer->Read(&consume_ret, &consume_err);
    if (consume_ret == KafkaConsumer::CONSUMER_PARTITION_EOF) {
      if (g_transaction) delete g_transaction;
      g_transaction = NULL;
      return TOPIC_CHECK_FINISHED;
    } else if (consume_ret == KafkaConsumer::CONSUMER_TIMED_OUT) {
      gettimeofday(&curr_time, NULL);
      if (GetIntervalMs(&curr_time, &last_success) >= 10 * 1000) {
        RDP_LOG_INFO << "Consumer Timeout > 10 Second, Exit";
        if (g_transaction) delete g_transaction;
        g_transaction = NULL;
        return TOPIC_CHECK_TIMEOUT;
      }
      continue;
    } else if (consume_ret != KafkaConsumer::CONSUMER_NO_ERROR) {
      RDP_LOG_ERROR << "Kafka consumer failed, last offset:" << cur_offset
                    << ", error code:" << consume_ret << ", error message:" << consume_err;
      if (g_transaction) delete g_transaction;
      g_transaction = NULL;
      return TOPIC_CONSUMER_ERROR;
    }

    gettimeofday(&last_success, NULL);

    rate_limiter.Wait(pkg_kfk->len());

    cur_offset = pkg_kfk->offset();

    pkg_rdp = PkgKfkToRdp(pkg_kfk);

    // epoch       : dynasty for rdp
    // trans_seq_no: transaction sequence in epoch
    // seq_no      : split sequence in transaction
    // split_flag  : 0-> 独立数据，没有分包操作; 1-> 分包数据，非最后分包; 2-> 分包数据，最后分包
    if (pkg_rdp->epoch() < g_epoch) {
      SkipPkg(cur_offset, pkg_rdp);
      continue;
    }
    if (pkg_rdp->epoch() == g_epoch) {
      if (pkg_rdp->trans_seq_no() < g_trans_seq_no) {
        SkipPkg(cur_offset, pkg_rdp);
        continue;
      }
      if (pkg_rdp->trans_seq_no() == g_trans_seq_no) {
        if (pkg_rdp->split_flag() == PKG_NOT_SPLIT) {
          SkipPkg(cur_offset, pkg_rdp);
          continue;
        }
        if (pkg_rdp->seq_no() != (g_split_seq_no + 1)) {
          SkipPkg(cur_offset, pkg_rdp);
          continue;
        }  // split pkg, but is duplicate
        // Normal pkg is a split package after the first , with incremental seq_no
        //data.append(pkg_rdp->data());
        if (NULL == (g_transaction = KafkaPkgToTransaction(pkg_rdp, g_transaction))) {
          RDP_LOG_ERROR << "Kafka Pkg to transaction error";
          if (g_transaction) delete g_transaction;
          g_transaction = NULL;
          return TOPIC_CHECK_PROTO_ERROR;
        }
      } else {
        //if (data.size() > 0) data.clear();  // clear the pre-data
         if (g_transaction != NULL) {
          delete g_transaction;
          g_transaction = NULL;
        }
        // Normal pkg, maybe it is a first split package
        if (pkg_rdp->seq_no() != 0) {
          SkipPkg(cur_offset, pkg_rdp);
          continue;
        }  // split pkg, but is duplicate
        if (g_trans_seq_no != 0 && pkg_rdp->trans_seq_no() != (g_trans_seq_no + 1)) {
          SkipPkg(cur_offset, pkg_rdp);
          continue;
        }  // split pkg, but is duplicate
        //data.append(pkg_rdp->data());
        if (NULL == (g_transaction = KafkaPkgToTransaction(pkg_rdp, g_transaction))) {
          RDP_LOG_ERROR << "Kafka Pkg to transaction error";
          if (g_transaction) delete g_transaction;
          g_transaction = NULL;
          return TOPIC_CHECK_PROTO_ERROR;
        }
      }
    } else {
      //if (data.size() > 0) data.clear();  // clear the pre-data
      if (g_transaction != NULL) {
        delete g_transaction;
        g_transaction = NULL;
      }
      // Normal pkg, maybe it is a first split package
      if (pkg_rdp->seq_no() != 0) {
        SkipPkg(cur_offset, pkg_rdp);
        continue;
      }  // split pkg, but is duplicate
      if (g_trans_seq_no != 0 && pkg_rdp->trans_seq_no() != (g_trans_seq_no + 1)) {
        SkipPkg(cur_offset, pkg_rdp);
        continue;
      }  // split pkg, but is duplicate
      //data.append(pkg_rdp->data());
      if (NULL == (g_transaction = KafkaPkgToTransaction(pkg_rdp, g_transaction))) {
        RDP_LOG_ERROR << "Kafka Pkg to transaction error";
        if (g_transaction) delete g_transaction;
        g_transaction = NULL;
        return TOPIC_CHECK_PROTO_ERROR;
      }
    }

    g_epoch = pkg_rdp->epoch();
    g_trans_seq_no = pkg_rdp->trans_seq_no();
    g_split_seq_no = pkg_rdp->seq_no();
    g_last_split_flag = pkg_rdp->split_flag();

    if (pkg_rdp->split_flag() != 1) {
      break;
    }
  }

  if (pkg_kfk) {
    delete pkg_kfk;
    pkg_kfk = NULL;
  }

  if (pkg_rdp) {
    delete pkg_rdp;
    pkg_rdp = NULL;
  }

  if (g_last_split_flag == 1) {
    RDP_LOG_INFO << "offset[" << g_last_offset << "," << cur_offset
                 << ") which is part of transaction, is skipped";
    if (g_transaction) delete g_transaction;
    g_transaction = NULL;
    return TOPIC_CHECK_FINISHED;
  }

  packing_msg->transaction = new rdp::messages::Transaction();
  *(packing_msg->transaction) = *g_transaction;
  packing_msg->begin_offset = g_last_offset + 1;// 上一个事务结束的offset + 1,
  // 注：上一个事务与这个事务之间的未完整分包也算在这个事务之中统计
  packing_msg->end_offset = cur_offset;
  packing_msg->epoch = g_epoch;

  g_last_offset = cur_offset;

  return 0;
}

bool Process() {
  PackingMsg *packing_msg = NULL;
  int32_t ret = 0;

  // initialize kafka consumer
  if (!ConsumerInit()) {
    RDP_LOG_ERROR << "Initialize Kafka Consumer Failt";
    goto err;
  }

  // revise g_start_offset and g_stop_offset use kafka topic low and high offset
  if (!ReviseStartStopOffset()) {
    RDP_LOG_ERROR << "Revise Kafka Consumer Start And Stop Offset Failt";
    goto err;
  }

  // start consume kafka message
  if (!g_consumer->Start(g_start_offset)) {
    RDP_LOG_ERROR << "Start Consumer At Offset: " << g_start_offset << " Failt";
    goto err;
  }

  do {
    packing_msg = new PackingMsg;
    assert(packing_msg != NULL);

    ret = GetNextPkg(packing_msg);
    if (ret) {
      // vms消息消费完了，正常结束
      if (ret == TOPIC_CHECK_FINISHED) break;

      //错误结束
      goto err;
    }

    // check PackingMsg is Match Serial
    if (!ProcessPackMsg(packing_msg)) {
      RDP_LOG_ERROR << "ProcessPackMsg Failt";
      goto err;
    }
    //delete packing_msg;
    //packing_msg = NULL;
  } while (true);

  if (packing_msg) {
    delete packing_msg;
    packing_msg = NULL;
  }

  ConsumerDestroy();

  return 0 == ret || TOPIC_CHECK_FINISHED == ret;

err:
  if (packing_msg) {
    delete packing_msg;
    packing_msg = NULL;
  }

  ConsumerDestroy();

  return false;
}

bool ProcessPackMsg(PackingMsg *msg) {
  MatchSerial(msg);

  if (g_out_csv) {
    PrintToCsv(msg);
  }
  return true;
}

bool ConsumerInit() {
  if (NULL != g_consumer) {
    return true;
  }

  string kafka_debug = "";
  g_consumer = new KafkaConsumer();
  if (NULL == g_consumer) {
    RDP_LOG_ERROR << "New KafkaConsumer Failt: " << GetLastErrorMsg(errno);
    goto err;
  }

  if (g_kfk_debug) {
    kafka_debug = "all";
  }

  if (!g_consumer->Init(g_brokers, g_topic, g_partition, g_b_version_fback, g_fetch_max_msg_bytes,
                        kafka_debug)) {
    RDP_LOG_ERROR << "Init Kafka Consumer, Brokerlist: " << g_brokers << ", Topic: " << g_topic
                  << ", Partition: " << g_partition
                  << ", Fetch Max Msg Bytes: " << g_fetch_max_msg_bytes
                  << ", Broker Version Fallback:" << g_b_version_fback;
    goto err;
  }

  return true;

err:
  ConsumerDestroy();
  return false;
}

void ConsumerDestroy() {
  if (NULL == g_consumer) {
    return;
  }
  delete g_consumer;
  g_consumer = NULL;
}

bool ReviseStartStopOffset() {
  int64_t topic_begin_offset = 0;
  int64_t topic_end_offset = 0;

  if (!g_consumer->QueryOffset(&topic_begin_offset, &topic_end_offset)) {
    RDP_LOG_ERROR << "Consumer Get Low And High Offset Failt!";
    return false;
  }
  RDP_LOG_INFO << "Topic: " << g_topic << ", Partition: " << g_partition
               << ", Offset Low: " << topic_begin_offset << ", High: " << topic_end_offset;

  if (g_check_topic_all_data) {
    g_start_offset = topic_begin_offset;
    g_stop_offset = topic_end_offset;
  } else {
    if (g_start_offset < topic_begin_offset || g_stop_offset > topic_end_offset) {
      RDP_LOG_ERROR << "Start Offset Or Stop Offset Is Not In [" << topic_begin_offset << ":"
                    << topic_end_offset << ")";
      return false;
    }
  }

  return true;
}

rdp::messages::KafkaPkg *PkgKfkToRdp(RdKafka::Message *msg) {

  ::rdp::messages::VMSMessage *vms_msg = NULL;
  ::rdp::messages::KafkaPkg *pkg = NULL;

  vms_msg = new ::rdp::messages::VMSMessage();
  assert(NULL != vms_msg);

  // decode message to VMSMessage
  if (!vms_msg->ParseFromArray(msg->payload(), msg->len())) {
    RDP_LOG_ERROR << "Decode Brokerlist: " << g_brokers << ", Topic: " << g_topic
                  << ", Partition: " << g_partition << ", Offset: " << msg->offset() << " Failt";
    goto err;
  }

  pkg = new ::rdp::messages::KafkaPkg;
  assert(NULL != pkg);

  // decode VMSMessage to KafkaPkg
  if (!pkg->ParseFromString(vms_msg->payload())) {
    RDP_LOG_ERROR << "Decode VMSMessage: " << g_brokers << ", Topic: " << g_topic
                  << ", Partition: " << g_partition << ", Offset: " << msg->offset() << " Failt";
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
