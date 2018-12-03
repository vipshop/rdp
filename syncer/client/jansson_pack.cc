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

#include <rdp-comm/logger.h>
#include "jansson_pack.h"

namespace syncer {

string CPJansson::title_flag_ = "flag";
string CPJansson::title_timestamp_ = "timestamp";
string CPJansson::title_topic_ = "topic";
string CPJansson::title_offset_ = "offset";
string CPJansson::title_gtid_ = "gtid";
string CPJansson::title_epoch_ = "epoch";
string CPJansson::title_seq_no_ = "seq_no";

json_t *CPJansson::json_seq_no_= NULL;
json_t *CPJansson::json_epoch_ = NULL;
json_t *CPJansson::json_gtid_ = NULL;
json_t *CPJansson::json_flag_ = NULL;
json_t *CPJansson::json_topic_ = NULL;
json_t *CPJansson::json_offset_ = NULL;
json_t *CPJansson::json_timestamp_ = NULL;
json_t *CPJansson::json_ = NULL;

char *CPJansson::encode_buffer_ = NULL;
uint32_t CPJansson::encode_buffer_size_ = 0;

bool CPJansson::init_flag_ = false;

bool CPJansson::Init() {
  if (init_flag_) {
    return true;
  }

  if (NULL == (json_ = json_object())) {
    RDP_LOG_ERROR << "New json_object Failt: " << GetLastErrorMsg(errno);
    goto err;
  }

  if (NULL == (json_seq_no_ = json_string(""))) {
    RDP_LOG_ERROR << "New json_string Failt: " << GetLastErrorMsg(errno);
    goto err;
  }

  if (NULL == (json_epoch_ = json_string(""))) {
    RDP_LOG_ERROR << "New json_string Failt: " << GetLastErrorMsg(errno);
    goto err;
  }

  if (NULL == (json_gtid_ = json_string(""))) {
    RDP_LOG_ERROR << "New json_string Failt: " << GetLastErrorMsg(errno);
    goto err;
  }

  if (NULL == (json_topic_ = json_string(""))) {
    RDP_LOG_ERROR << "New json_string Failt: " << GetLastErrorMsg(errno);
    goto err;
  }

  if (NULL == (json_offset_ = json_string(""))) {
    RDP_LOG_ERROR << "New json_string Failt: " << GetLastErrorMsg(errno);
    goto err;
  }

  if (NULL == (json_flag_ = json_string(""))) {
    RDP_LOG_ERROR << "New json_string Failt: " << GetLastErrorMsg(errno);
    goto err;
  }

  if (NULL == (json_timestamp_ = json_string(""))) {
    RDP_LOG_ERROR << "New json_string Failt: " << GetLastErrorMsg(errno);
    goto err;
  }

  if (0 != json_object_set(json_, title_flag_.c_str(), json_flag_)) {
    RDP_LOG_ERROR << "json_object_set " << title_flag_ << " Failt";
    goto err;
  }

  if (0 != json_object_set(json_, title_topic_.c_str(), json_topic_)) {
    RDP_LOG_ERROR << "json_object_set " << title_topic_ << " Failt";
    goto err;
  }

  if (0 != json_object_set(json_, title_offset_.c_str(), json_offset_)) {
    RDP_LOG_ERROR << "json_object_set " << title_offset_ << " Failt";
    goto err;
  }

  if (0 != json_object_set(json_, title_gtid_.c_str(), json_gtid_)) {
    RDP_LOG_ERROR << "json_object_set " << title_gtid_ << " Failt";
    goto err;
  }

  if (0 != json_object_set(json_, title_epoch_.c_str(), json_epoch_)) {
    RDP_LOG_ERROR << "json_object_set " << title_epoch_ << " Failt";
    goto err;
  }

  if (0 != json_object_set(json_, title_seq_no_.c_str(), json_seq_no_)) {
    RDP_LOG_ERROR << "json_object_set " << title_seq_no_ << " Failt";
    goto err;
  }

  if (0 != json_object_set(json_, title_timestamp_.c_str(), json_timestamp_)) {
    RDP_LOG_ERROR << "json_object_set " << title_timestamp_ << " Failt";
    goto err;
  }

  encode_buffer_ = (char *)calloc(sizeof(char), BUFFER_SIZE);
  if (NULL == encode_buffer_) {
    RDP_LOG_ERROR << "Calloc Encoder Buffer Error: " << GetLastErrorMsg(errno);
    goto err;
  }
  encode_buffer_size_ = BUFFER_SIZE;

  init_flag_ = true;

  return true;

err:
  if (json_) {
    delete json_;
    json_ = NULL;
  }

  if (json_topic_) {
    delete json_topic_;
    json_topic_ = NULL;
  }

  if (json_flag_) {
    delete json_flag_;
    json_flag_ = NULL;
  }

  if (json_offset_) {
    delete json_offset_;
    json_offset_ = NULL;
  }

  if (json_gtid_) {
    delete json_gtid_;
    json_gtid_ = NULL;
  }

  if (json_epoch_) {
    delete json_epoch_;
    json_epoch_ = NULL;
  }

  if (json_seq_no_) {
    delete json_seq_no_;
    json_seq_no_ = NULL;
  }

  if (json_timestamp_) {
    delete json_timestamp_;
    json_timestamp_ = NULL;
  }

  if (encode_buffer_) {
    free(encode_buffer_);
    encode_buffer_ = NULL;
  }
  encode_buffer_size_ = 0;

  init_flag_ = false;

  return false;
}

bool CPJansson::Encode(const string &topic, const string &offset,
                       const string &flag, const string &timestamp,
                       const string &gtid, const string &epoch,
                       const string &seq_no, string *rs) {
  assert(NULL != rs);
  size_t len = 0;
  if (!init_flag_ && !Init()) {
    RDP_LOG_ERROR << "Jansson Init Failt";
    return false;
  }

  if (0 != json_string_set(json_topic_, topic.c_str())) {
    RDP_LOG_ERROR << "Json Set: " << title_topic_ << ", Value: " << topic
               << " Failt";
    goto err;
  }

  if (0 != json_string_set(json_offset_, offset.c_str())) {
    RDP_LOG_ERROR << "Json Set: " << title_offset_ << ", Value: " << offset
               << " Failt";
    goto err;
  }

  if (0 != json_string_set(json_flag_, flag.c_str())) {
    RDP_LOG_ERROR << "Json Set: " << title_flag_ << ", Value: " << flag
               << " Failt";
    goto err;
  }

  if (0 != json_string_set(json_timestamp_, timestamp.c_str())) {
    RDP_LOG_ERROR << "Json Set: " << title_timestamp_ << ", Value: " << timestamp
               << " Failt";
    goto err;
  }

  if (0 != json_string_set(json_gtid_, gtid.c_str())) {
    RDP_LOG_ERROR << "Json Set: " << title_gtid_ << ", Value: " << gtid
               << " Failt";
    goto err;
  }

  if (0 != json_string_set(json_epoch_, epoch.c_str())) {
    RDP_LOG_ERROR << "Json Set: " << title_epoch_ << ", Value: " << epoch
               << " Failt";
    goto err;
  }

  if (0 != json_string_set(json_seq_no_, seq_no.c_str())) {
    RDP_LOG_ERROR << "Json Set: " << title_seq_no_ << ", Value: " << seq_no
               << " Failt";
    goto err;
  }

  // make sure enough buffer for json encode
  // 5*5+4+2+1=32 so 48 is enough
  if ((AllTitleLen() + topic.length() + offset.length() + flag.length() +
       timestamp.length() + gtid.length() + 48) > encode_buffer_size_) {
    encode_buffer_size_ *= 2;
    encode_buffer_ =
        (char *)realloc(encode_buffer_, sizeof(char) * encode_buffer_size_);
    if (NULL == encode_buffer_) {
      RDP_LOG_ERROR << "Realloc Encode Buffer Error: " << GetLastErrorMsg(errno);
      goto err;
    }
  }

  len = json_dumpb(json_, encode_buffer_, encode_buffer_size_ - 1,
                   JSON_COMPACT | JSON_ESCAPE_SLASH | JSON_SORT_KEYS);
  encode_buffer_[len] = '\0';

  rs->assign(encode_buffer_);

  return true;

err:
  rs->clear();
  return false;
}

bool CPJansson::Decode(const string &source, string *topic, string *offset,
                       string *flag, string *timestamp, string *gtid, string *epoch, string *seq_no) {
  assert(NULL != topic && NULL != offset && NULL != flag && NULL != timestamp &&
         NULL != gtid && NULL != epoch && NULL != seq_no);

  topic->clear();
  offset->clear();
  flag->clear();
  timestamp->clear();
  gtid->clear();
  epoch->clear();
  seq_no->clear();

  if (source.empty()) {
    return true;
  }

  json_t *json_de = NULL;
  json_error_t json_err;
  const char *key = NULL;
  json_t *value = NULL;

  json_de =
      json_loadb(source.c_str(), source.length(),
                 JSON_DECODE_INT_AS_REAL | JSON_REJECT_DUPLICATES, &json_err);
  if (!json_de) {
    RDP_LOG_ERROR << "Json Decode: " << source << ", Falt: " << json_err.text;
    json_decref(json_de);
    return false;
  }
  json_object_foreach(json_de, key, value) {
    if (0 == title_timestamp_.compare(key)) {
      timestamp->assign(json_string_value(value));
      continue;
    }

    if (0 == title_flag_.compare(key)) {
      flag->assign(json_string_value(value));
      continue;
    }

    if (0 == title_topic_.compare(key)) {
      topic->assign(json_string_value(value));
      continue;
    }

    if (0 == title_offset_.compare(key)) {
      offset->assign(json_string_value(value));
      continue;
    }

    if (0 == title_gtid_.compare(key)) {
      gtid->assign(json_string_value(value));
      continue;
    }

    if (0 == title_epoch_.compare(key)) {
      epoch->assign(json_string_value(value));
      continue;
    }

    if (0 == title_seq_no_.compare(key)) {
      seq_no->assign(json_string_value(value));
      continue;
    }
  }
  json_decref(json_de);
  return true;
}

uint32_t CPJansson::AllTitleLen() {
  static uint32_t rs = 0;
  if (0 == rs) {
    rs += title_flag_.length();
    rs += title_timestamp_.length();
    rs += title_topic_.length();
    rs += title_offset_.length();
    rs += title_gtid_.length();
  }
  return rs;
}

}  // namespace syncer
