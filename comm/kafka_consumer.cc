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

#include "kafka_consumer.h"

namespace syncer {
KafkaConsumer::KafkaConsumer() {
  kafka_conf_ = NULL;
  topic_conf_ = NULL;
  consumer_ = NULL;
  topic_ = NULL;
  partition_ = 0;
}

KafkaConsumer::~KafkaConsumer() { Reset(); }

bool KafkaConsumer::Init(const string &brokers_list, const string &topic_name,
                         const unsigned int partition,
                         const string &b_version_fback,
                         const uint32_t fetch_max_msg_bytes,
                         const string &debug) {
  Reset();

  string errstr;

  this->topic_name_ = topic_name;

  kafka_conf_ = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
  topic_conf_ = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);

  if (RdKafka::Conf::CONF_OK !=
      kafka_conf_->set("metadata.broker.list", brokers_list, errstr)) {
    RDP_LOG_ERROR << "Kafka Consumer Set Configure 'metadata.broker.list': "
               << brokers_list << ", Failt: " << errstr;
    goto err;
  }

  if (!debug.empty() && RdKafka::Conf::CONF_OK !=
      kafka_conf_->set("debug", debug, errstr)) {
    RDP_LOG_ERROR << "Kafka Consumer Set Configure 'debug': "
               << debug << ", Failt: " << errstr;
    goto err;
  }

  if (fetch_max_msg_bytes > 0) {
    char fetch_max_msg_str[64] = {0x0};
    snprintf(fetch_max_msg_str, sizeof(fetch_max_msg_str) - 1, "%" PRIu32,
             fetch_max_msg_bytes);
    if (RdKafka::Conf::CONF_OK !=
        kafka_conf_->set("fetch.message.max.bytes",
                         string(fetch_max_msg_str, strlen(fetch_max_msg_str)),
                         errstr)) {
      RDP_LOG_ERROR << "Kafka Consumer Set Configure 'fetch.message.max.bytes': "
                 << fetch_max_msg_bytes << ", Failt: " << errstr;
      goto err;
    }
  }

  if (RdKafka::Conf::CONF_OK !=
      kafka_conf_->set("broker.version.fallback", b_version_fback, errstr)) {
    RDP_LOG_ERROR << "Kafka Consumer Set Configure 'broker.version.fallback': "
               << b_version_fback << ", Failt: " << errstr;
    goto err;
  }

  consumer_ = RdKafka::Consumer::create(kafka_conf_, errstr);
  if (!consumer_) {
    RDP_LOG_ERROR << "Create Kafka Consumer Failt:" << errstr;
    goto err;
  }

  topic_ = RdKafka::Topic::create(consumer_, topic_name, topic_conf_, errstr);
  if (!topic_) {
    RDP_LOG_ERROR << "Create Kafka Topic:" << topic_name << " Failt:" << errstr;
    goto err;
  }

  RDP_LOG_INFO << "Created Consumer " << consumer_->name()
            << " Of Topic:" << topic_name << " Partition:" << partition;

  partition_ = partition;

  return true;

err:
  Reset();
  return false;
}

void KafkaConsumer::Reset() {
  if (consumer_) {
    consumer_->stop(topic_, partition_);
    consumer_->poll(1000);
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
  if (consumer_) {
    delete consumer_;
    consumer_ = NULL;
  }
  RdKafka::wait_destroyed(5000);
}

RdKafka::Message *KafkaConsumer::Read(int *rs, string *errstr) {
  assert(NULL != rs && NULL != errstr);

  if (NULL == consumer_) {
    *rs = CONSUMER_NOT_INIT;
    return NULL;
  }

  RdKafka::Message *msg = consumer_->consume(topic_, partition_, 3000);

  // consume kafka message meet error
  if (RdKafka::ERR_NO_ERROR == msg->err()) {
    *rs = CONSUMER_NO_ERROR;
    goto end;
  }

  // consume kafka message reach topic end
  if (RdKafka::ERR__PARTITION_EOF == msg->err()) {
    RDP_LOG_INFO << "Kafka Consumer: " << consumer_->name()
              << " Reach Topic: " << topic_->name()
              << ", Partition: " << partition_ << " End";
    msg = NULL;
    *rs = CONSUMER_PARTITION_EOF;
    goto end;
  }

  // consume kafka message meet timeout, continue consume
  if (RdKafka::ERR__TIMED_OUT == msg->err()) {
    msg = NULL;
    *rs = CONSUMER_TIMED_OUT;
    goto end;
  }

  RDP_LOG_ERROR << "Kafka Consumer: " << consumer_->name()
             << " Reach Topic: " << topic_->name()
             << ", Partition: " << partition_
             << " Meer Error: " << msg->errstr();

  msg = NULL;

end:
  consumer_->poll(0);

  // return error string
  if (msg && *rs != CONSUMER_NO_ERROR) {
    errstr->assign(msg->errstr());
  }
  return msg;
}


bool KafkaConsumer::QueryOffset(int64_t *begin_offset, int64_t *end_offset) {
  if (!consumer_) {
    RDP_LOG_ERROR << "Consumer Not Init!";
    return false;
  }

  RdKafka::ErrorCode ret = consumer_->query_watermark_offsets(
      topic_name_, partition_, begin_offset, end_offset, 5000);
  if (ret != RdKafka::ERR_NO_ERROR) {
    RDP_LOG_ERROR << RdKafka::err2str(ret) << ", ret: " << ret;
    return false;
  }

  if (*begin_offset == RdKafka::Topic::OFFSET_INVALID ||
      *end_offset == RdKafka::Topic::OFFSET_INVALID) {
    RDP_LOG_ERROR << "Consumer Start Or End Offset Is Invalid";
    return false;
  }

  return true;
}

bool KafkaConsumer::Start(const int64_t offset) {
  if (consumer_ == NULL) {
    RDP_LOG_ERROR << "Consumer Not Init";
    return false;
  }

  RdKafka::ErrorCode ret = consumer_->start(topic_, partition_, offset);
  if (RdKafka::ERR_NO_ERROR != ret) {
    RDP_LOG_ERROR << "Stop Consumer Failt:" << RdKafka::err2str(ret)
                  << ", ret: " << ret;
    return false;
  }

  RDP_LOG_INFO << "Start Kafka Consumer";
  return true;
}

bool KafkaConsumer::Stop() {
  if (consumer_ == NULL) {
    RDP_LOG_ERROR << "Consumer Not Init";
    return false;
  }

  RdKafka::ErrorCode ret = consumer_->stop(topic_, partition_);
  if (RdKafka::ERR_NO_ERROR != ret) {
    RDP_LOG_ERROR << "Stop Consumer Failt:" << RdKafka::err2str(ret)
                  << ", ret: " << ret;
    return false;
  }

  return true; 
}

}  // namespace syncer
