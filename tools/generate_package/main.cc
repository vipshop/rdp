/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2014, Magnus Edenhill
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

/**
 * Apache Kafka consumer & producer example programs
 * using the Kafka driver from librdkafka
 * (https://github.com/edenhill/librdkafka)
 */

#include <iostream>
#include <string>
#include <cstdlib>
#include <cstdio>
#include <csignal>
#include <cstring>
#include <vector>

#include <rdp.pb.h>
#include <lz4.h>

#include <getopt.h>

/*
 * Typically include path in a real application would be
 * #include <librdkafka/rdkafkacpp.h>
 */
#include <rdkafkacpp.h>
#include "json2pb.h"
#include <jansson.h>

using std::vector;
using std::string;

/* Use of this partitioner is pretty pointless since no key is provided
 * in the produce() call. */
class MyHashPartitionerCb : public RdKafka::PartitionerCb {
 public:
  int32_t partitioner_cb(const RdKafka::Topic *topic, const std::string *key,
                         int32_t partition_cnt, void *msg_opaque) {
    (void)topic, (void)key, (void)msg_opaque;                         
    return djb_hash(key->c_str(), key->size()) % partition_cnt;
  }

 private:
  static inline unsigned int djb_hash(const char *str, size_t len) {
    unsigned int hash = 5381;
    for (size_t i = 0; i < len; i++) hash = ((hash << 5) + hash) + str[i];
    return hash;
  }
};

std::string g_topic_str;
RdKafka::Conf *g_conf = NULL;
RdKafka::Conf *g_tconf = NULL;
std::string g_brokers = "localhost";
std::string g_b_version_fback;
std::string g_debug;
bool g_do_conf_dump = false;
MyHashPartitionerCb g_hash_partitioner;
int32_t g_partition = RdKafka::Topic::PARTITION_UA;
int g_use_ccb = 0;

uint32_t g_split_count = 1;
uint32_t g_split_idx;
uint32_t g_epoch = 1;
std::string g_trans = "";
uint64_t g_start_position = -1;

bool g_enable_compress = false;
char *g_compress_buf = NULL;
size_t g_compress_buf_size = 0;

static bool run = true;

int g_kfkpkg_version = -1;

static void sigterm(int sig) { (void) sig; run = false; }

#define ALLOC_BUF(buf, need_size, buf_size)                   \
  do {                                                        \
    if ((buf) == NULL) {                                      \
      if ((buf_size) < (need_size)) (buf_size) = (need_size); \
      (buf) = (char *)malloc((need_size));                    \
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

static int Compress(const char *src, const size_t src_size, char **dst, size_t *dst_size) {
  // 获取压缩后空间
  const int max_dst_size = LZ4_compressBound(src_size);
  // 确保空间足够
  ALLOC_BUF(*dst, max_dst_size, *dst_size);
  int retry = 0;
  int compressed_data_size = 0;
  // 压缩 失败后重试
  do {
    compressed_data_size = LZ4_compress_default(src, *dst, src_size, *dst_size);
    // 压缩失败
    if (compressed_data_size < 0)
      std::cerr << "A negative result from LZ4_compress_default "
                       "indicates a failure trying to compress the data.  See exit code (echo "
                    << compressed_data_size << ") for value returned." << std::endl;
    // 空间不足 压缩失败
    // 一般情况不会进入里面，因为LZ4_compressBound已经计算压缩后空间
    if (compressed_data_size == 0) {
      std::cout << "A result of 0 means compression worked, but was stopped because the "
                "destination buffer couldn't hold all the information." << std::endl;
      ALLOC_BUF(*dst, (*dst_size) * 2, *dst_size);
    }
  } while (compressed_data_size == 0 && retry++ < 3);

  if (compressed_data_size == 0) {
    std::cerr << "compress stopped because thedestination buffer couldn't hold all the "
                     "information. compress buf size: " << *dst_size << std::endl;
  }
	return compressed_data_size;
}

class ExampleDeliveryReportCb : public RdKafka::DeliveryReportCb {
 public:
  void dr_cb(RdKafka::Message &message) {
    std::cout << "Message delivery for (" << message.len()
              << " bytes): " << message.errstr() << std::endl;
    if (message.key())
      std::cout << "Key: " << *(message.key()) << ";" << std::endl;
  }
};

class ExampleEventCb : public RdKafka::EventCb {
 public:
  void event_cb(RdKafka::Event &event) {
    switch (event.type()) {
      case RdKafka::Event::EVENT_ERROR:
        std::cerr << "ERROR (" << RdKafka::err2str(event.err())
                  << "): " << event.str() << std::endl;
        if (event.err() == RdKafka::ERR__ALL_BROKERS_DOWN) run = false;
        break;

      case RdKafka::Event::EVENT_STATS:
        std::cerr << "\"STATS\": " << event.str() << std::endl;
        break;

      case RdKafka::Event::EVENT_LOG:
        fprintf(stderr, "LOG-%i-%s: %s\n", event.severity(),
                event.fac().c_str(), event.str().c_str());
        break;

      default:
        std::cerr << "EVENT " << event.type() << " ("
                  << RdKafka::err2str(event.err()) << "): " << event.str()
                  << std::endl;
        break;
    }
  }
};

int32_t print_usage() {
    std::string features;
    g_conf->get("builtin.features", features);
    fprintf(stderr,
            "Usage: %s -t <topic> "
            "[-p <partition>] [-b <host1:port1,host2:port2,..>]\n"
            "\n"
            "librdkafka version %s (0x%08x, builtin.features \"%s\")\n"
            "\n"
            " Options:\n"
            "  -t <topic>      Topic to fetch / produce\n"
            "  -p <num>        Partition (random partitioner)\n"
            "  -p <func>       Use partitioner:\n"
            "                  random (default), hash\n"
            "  -b <brokers>    Broker address (localhost:9092)\n"
            "  -z <codec>      Enable compression:\n"
            "                  none|gzip|snappy\n"
            "  -d [facs..]     Enable debugging contexts:\n"
            "                  %s\n"
            "  -M <intervalms> Enable statistics\n"
            "  -X <prop=name>  Set arbitrary librdkafka "
            "configuration property\n"
            "                  Properties prefixed with \"topic.\" "
            "will be set on topic object.\n"
            "                  Use '-X list' to see the full list\n"
            "                  of supported properties.\n"
            "  -f <flag>       Set option:\n"
            "                     ccb - use consume_callback\n"
            "  -y <broker.version.fallback>  eg: 0.8.1\n"
            "  -c <split count>  \n"
            "  -i <split index>  \n"
            "  -e <epoch>  \n"
            "  -s <start position>  \n"
            "  -T <transaction json string>  \n"
            "  -Z compress transaction  \n"
            "  -v <kafka pkg version>  \n"
            "\n"
            "\n"
            "\n"
            "\n",
            "./gennerate_package", RdKafka::version_str().c_str(), RdKafka::version(),
            features.c_str(), RdKafka::get_debug_contexts().c_str());
    return 1;
}

std::vector<std::string> split(std::string str,std::string pattern)  
{  
  std::string::size_type pos;  
  std::vector<std::string> result;  
  str+=pattern;//扩展字符串以方便操作  
  std::string::size_type size=str.size();  

  for(uint32_t i=0; i<size; i++)  
  {  
    pos=str.find(pattern,i);  
    if(pos<size)  
    {  
      std::string s=str.substr(i,pos-i);  
      result.push_back(s);  
      i=pos+pattern.size()-1;  
    }  
  }  
  return result;  
}  

vector<uint32_t> get_split_idx(string opt_value) {
  std::vector<std::string> result = split(opt_value, ",");
  std::vector<uint32_t> split_idx;
  for (uint32_t i = 0; i < result.size(); i++) {
    uint32_t idx = std::atoi(result[i].c_str());
    split_idx.push_back(idx);
  }

  return split_idx;
}

int32_t parse_option(int argc, char **argv) {
  int opt;
  std::string errstr;
  while ((opt = getopt(argc, argv, "t:p:b:z:dX:M:f:y:c:i:e:T:s:v:Z")) != -1) {
    switch (opt) {
      case 't':
        g_topic_str = optarg;
        break;

      case 'p':
        if (!strcmp(optarg, "random"))
          /* default */;
        else if (!strcmp(optarg, "hash")) {
          if (g_tconf->set("partitioner_cb", &g_hash_partitioner, errstr) !=
              RdKafka::Conf::CONF_OK) {
            std::cerr << errstr << std::endl;
            exit(1);
          }
        } else
          g_partition = std::atoi(optarg);
        break;

      case 'b':
        g_brokers = optarg;
        break;

      case 'z':
        if (g_conf->set("compression.codec", optarg, errstr) !=
            RdKafka::Conf::CONF_OK) {
          std::cerr << errstr << std::endl;
          exit(1);
        }
        break;

      case 'd':
        g_debug = optarg;
        break;

      case 'M':
        if (g_conf->set("statistics.interval.ms", optarg, errstr) !=
            RdKafka::Conf::CONF_OK) {
          std::cerr << errstr << std::endl;
          exit(1);
        }
        break;

      case 'X': {
        char *name, *val;

        if (!strcmp(optarg, "dump")) {
          g_do_conf_dump = true;
          continue;
        }

        name = optarg;
        if (!(val = strchr(name, '='))) {
          std::cerr << "%% Expected -X property=value, not " << name
                    << std::endl;
          exit(1);
        }

        *val = '\0';
        val++;

        /* Try "topic." prefixed properties on topic
         * conf first, and then fall through to global if
         * it didnt match a topic configuration property. */
        RdKafka::Conf::ConfResult res;
        if (!strncmp(name, "topic.", strlen("topic.")))
          res = g_tconf->set(name + strlen("topic."), val, errstr);
        else
          res = g_conf->set(name, val, errstr);

        if (res != RdKafka::Conf::CONF_OK) {
          std::cerr << errstr << std::endl;
          exit(1);
        }
      } break;

      case 'f':
        if (!strcmp(optarg, "ccb"))
          g_use_ccb = 1;
        else {
          std::cerr << "Unknown option: " << optarg << std::endl;
          exit(1);
        }
        break;

      case 'y':
        g_b_version_fback = optarg;

        if (RdKafka::Conf::CONF_OK !=
            g_conf->set("broker.version.fallback", g_b_version_fback, errstr)) {
          std::cerr
              << "Kafka Consumer Set Configure 'broker.version.fallback': "
              << g_b_version_fback << ", Failt: " << errstr << std::endl;
          ;
          exit(1);
        }
        break;
      case 'c':
        g_split_count = std::atoi(optarg);
        break;

      case 'i':
        g_split_idx = std::atoi(optarg);
        break;

      case 'e':
        g_epoch = std::atoi(optarg);
        break;

      case 'T':
        g_trans = optarg;
        break;

      case 's':
        g_start_position = std::atoi(optarg);
        break;

      case 'Z':
        g_enable_compress = true;
			  ALLOC_BUF(g_compress_buf, 5 * 1024 * 1024 * sizeof(char), g_compress_buf_size);	
        break;

      case 'v':
        g_kfkpkg_version = std::atoi(optarg);
        break;

      default:
        return print_usage();
    }
  }

  if (g_topic_str.empty() || optind != argc) {
    return print_usage();
  }

  return 0;
}

void dump_conf() {
  int pass;

  for (pass = 0; pass < 2; pass++) {
    std::list<std::string> *dump;
    if (pass == 0) {
      dump = g_conf->dump();
      std::cout << "# Global config" << std::endl;
    } else {
      dump = g_tconf->dump();
      std::cout << "# Topic config" << std::endl;
    }

    for (std::list<std::string>::iterator it = dump->begin();
        it != dump->end();) {
      std::cout << *it << " = ";
      it++;
      std::cout << *it << std::endl;
      it++;
    }
    std::cout << std::endl;
  }

  return;
}

int32_t produce_package(RdKafka::Producer *producer, RdKafka::Topic *topic) {
  char *trans_buf = NULL;
  char *src_pkg_buf = NULL;
  rdp::messages::Transaction trans;
  rdp::messages::KafkaPkg kafka_pkg;
  rdp::messages::VMSMessage vms_message;
  uint32_t trans_buf_size;
  uint32_t kafka_pkg_flag = rdp::messages::kKfkPkgDefault;
  uint32_t kafka_buf_size;
  char *kafka_buf = NULL;
  uint32_t vms_buf_size;
  char *vms_buf = NULL;
  RdKafka::ErrorCode resp; 
  

  try {
    json2pb(trans, g_trans.c_str(), g_trans.size());
  } catch (std::exception &ex){
    std::cerr << "json2pb failed" << ex.what() << std::endl;
    goto error_t;
  }

  if (g_start_position >= 0) {
    int64_t position_diff = g_start_position - trans.position();
    trans.set_position(trans.position() + position_diff);
    trans.set_next_position(trans.next_position() + position_diff);

    for (int32_t i = 0; i < trans.events_size(); i++) {
      rdp::messages::Event *event = trans.mutable_events(i);
      event->set_position(event->position() + position_diff);
      event->set_next_position(event->next_position() + position_diff);
    }
  }

  trans_buf_size = trans.ByteSizeLong();
  trans_buf = new char[trans_buf_size];
  assert(trans_buf);
  if (!trans.SerializeToArray(trans_buf, trans_buf_size)) {
    std::cerr << "trans.SerializeToArray failed" << std::endl;
    goto error_t;
  }

  if (g_enable_compress) {
    trans_buf_size = Compress(trans_buf, trans_buf_size, &g_compress_buf, &g_compress_buf_size);
    if (trans_buf_size <= 0) {
      std::cerr << "Compress Failt" << std::endl;
      goto error_t;
    }
    delete trans_buf;
    trans_buf = g_compress_buf;
  }

  // transation to kafka
  kafka_pkg.set_epoch(g_epoch);
  kafka_pkg.set_trans_seq_no(trans.seq());
  kafka_pkg.set_gtid(trans.gtid());
  kafka_pkg.set_split_flag((1 == g_split_count)? 0 : ((g_split_idx < g_split_count - 1) ? 1 : 2));
  kafka_pkg.set_seq_no(g_split_idx);
  kafka_pkg.set_data(trans_buf, trans_buf_size);

  if (g_kfkpkg_version != -1)
    kafka_pkg.set_version((rdp::messages::PBVERSION)g_kfkpkg_version);

  if (g_enable_compress) {
    kafka_pkg_flag |= rdp::messages::kKfkPkgCompressData;
  }
  kafka_pkg.set_flag(kafka_pkg_flag);
  kafka_pkg.set_source_data_len(trans_buf_size);

  kafka_buf_size = kafka_pkg.ByteSizeLong();
  kafka_buf = new char[kafka_buf_size];
  assert(kafka_buf);

  if (!kafka_pkg.SerializeToArray(kafka_buf, kafka_buf_size)) {
    std::cerr << "kafka_pkg.SerializeToArray failed" << std::endl;
    delete kafka_buf;
    goto error_t;
  }

  //kafka to vms message
  vms_message.set_payload(kafka_buf, kafka_buf_size);

  vms_buf_size = vms_message.ByteSizeLong();
  vms_buf = new char[vms_buf_size];
  assert(vms_buf);

  if (!vms_message.SerializeToArray(vms_buf, vms_buf_size)) {
    std::cerr << "vms_pkg.SerializeToArray failed" << std::endl;
    delete vms_buf;
    delete kafka_buf;
    goto error_t;
  }

  resp = producer->produce(
      topic, g_partition, RdKafka::Producer::RK_MSG_COPY /* Copy payload */,
      vms_buf, vms_buf_size, NULL, NULL);
  if (resp != RdKafka::ERR_NO_ERROR) {
    std::cerr << "% Produce failed: " << RdKafka::err2str(resp)
      << std::endl;
    delete vms_buf;
    delete kafka_buf;
    goto error_t;
  }
  else
    std::cerr << "% Produced message (" << vms_buf_size << " bytes)"
      << std::endl;

  delete vms_buf;
  delete kafka_buf;


  producer->poll(0);

  if(!g_enable_compress && trans_buf) delete trans_buf;

  return 0;

error_t: 
  if(!g_enable_compress && trans_buf) delete trans_buf;
  if(src_pkg_buf) delete src_pkg_buf;
  return -1;

}

int main(int argc, char **argv) {
  std::string errstr;

  /*
   * Create configuration objects
   */
  g_conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
  g_tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);

  int32_t ret = parse_option(argc, argv);
  if (ret) return ret;

  /*
   * Set configuration properties
   */
  g_conf->set("metadata.broker.list", g_brokers, errstr);

  if (!g_debug.empty()) {
    if (g_conf->set("debug", g_debug, errstr) != RdKafka::Conf::CONF_OK) {
      std::cerr << errstr << std::endl;
      exit(1);
    }
  }

  ExampleEventCb ex_event_cb;
  g_conf->set("event_cb", &ex_event_cb, errstr);

  if (g_do_conf_dump) {
    dump_conf();
    exit(0);
  }

  signal(SIGINT, sigterm);
  signal(SIGTERM, sigterm);

  /*
   * Producer mode
   */

  if (g_topic_str.empty()) return print_usage();

  ExampleDeliveryReportCb ex_dr_cb;

  /* Set delivery report callback */
  g_conf->set("dr_cb", &ex_dr_cb, errstr);

  /*
   * Create producer using accumulated global configuration.
   */
  RdKafka::Producer *producer = RdKafka::Producer::create(g_conf, errstr);
  if (!producer) {
    std::cerr << "Failed to create producer: " << errstr << std::endl;
    exit(1);
  }

  std::cout << "% Created producer " << producer->name() << std::endl;

  /*
   * Create topic handle.
   */
  RdKafka::Topic *topic =
      RdKafka::Topic::create(producer, g_topic_str, g_tconf, errstr);
  if (!topic) {
    std::cerr << "Failed to create topic: " << errstr << std::endl;
    exit(1);
  }

  produce_package(producer, topic);

  while (run && producer->outq_len() > 0) {
    std::cerr << "Waiting for " << producer->outq_len() << std::endl;
    producer->poll(1000);
  }

  delete topic;
  delete producer;

  /*
   * Wait for RdKafka to decommission.
   * This is not strictly needed (when check outq_len() above), but
   * allows RdKafka to clean up all its resources before the application
   * exits so that memory profilers such as valgrind wont complain about
   * memory leaks.
   */
  RdKafka::wait_destroyed(5000);

  if (g_enable_compress && g_compress_buf != NULL) {
    FREE_BUF(g_compress_buf);
  }

  return 0;
}
