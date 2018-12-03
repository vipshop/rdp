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

#ifndef _KAFKA_CONSUMER_H_
#define _KAFKA_CONSUMER_H_

#define __STDC_FORMAT_MACROS
#include <inttypes.h>

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>
#include <librdkafka/rdkafkacpp.h>

#include "logger.h"

using std::string;

namespace syncer {

class KafkaConsumer {
 public:
  enum ReadRsCode {
    CONSUMER_NOT_INIT = -999,
    CONSUMER_NO_ERROR = RdKafka::ERR_NO_ERROR,
    CONSUMER_PARTITION_EOF = RdKafka::ERR__PARTITION_EOF,
    CONSUMER_TIMED_OUT = RdKafka::ERR__TIMED_OUT
  };

 public:
  explicit KafkaConsumer();
  ~KafkaConsumer();

  RdKafka::Message *Read(int *rs, string *errstr);

  bool Init(const string &brokers_list, const string &topic_name,
            const unsigned int partition, const string &b_version_fback,
            const uint32_t fetch_max_msg_bytes = 1048576, const string &debug="");

  void Reset(void);

  bool QueryOffset(int64_t *begin_offset, int64_t *end_offset);

  bool Start(const int64_t offset = RdKafka::Topic::OFFSET_BEGINNING);

  bool Stop();

 private:
  RdKafka::Conf *kafka_conf_;
  RdKafka::Conf *topic_conf_;
  RdKafka::Consumer *consumer_;
  RdKafka::Topic *topic_;
  string topic_name_;
  unsigned int partition_;
};

}  // namespace syncer
#endif  // _KAFKA_CONSUMER_H_
