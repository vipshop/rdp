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
#include "broken_points.h"
#include <vector>
#include <jansson.h>

using std::vector;

typedef vector<ErrOffsetSet *> ErrOffsetVector;
static ErrOffsetVector err_vec;

// 进入该函数的数据都是不通过连续性验证的
// 1.数据丢失
// 2.RDP切换:判断切换前后的事件是否连续，可校验通过则不为断点，否则为断点
// 3.binlog file切换同时RDP切换：情况比较复杂，直接报告为断点，人工看是否存在数据丢失
static ErrSetType IsErrSet(ErrOffsetSet *set) {
  return ERR_NO_ERROR;

  ErrOffset *before = &(set->before);
  ErrOffset *after = &(set->after);

  // Rotate 事件已经解析，所以不会出现由于binlog file切换导致的连续性校验错误的问题

  static ErrOffsetSet *previous_set = NULL;

  // 遇到RDP切换，但还没到正常Binlog数据
  if (previous_set != NULL) {
    // 如果再遇到一个断点，断点后为FORMAT_DESCRIPTION_EVENT事件且Epoch变大，则忽略，为RDP连续切换
    if (before->trans && after->trans && before->epoch < after->epoch) {
      const ::rdp::messages::Event &after_event = after->trans->events(0);
      if (after_event.event_type() == FORMAT_DESCRIPTION_EVENT) {
        // 需要保留previous_set，继续往下校验
        set->set_type = ERR_NO_ERROR;
        return ERR_NO_ERROR;
      }
    }
    // 判断previous_set断点的before和当前断点set的after是否连续，连续不为断点，不连续，报告两个断点
    before = &(previous_set->before);
    after = &(set->after);

    if (before->trans && after->trans &&
        before->trans->next_position() == after->trans->position() &&
        0 == before->trans->next_binlog_file_name().compare(after->trans->binlog_file_name())) {
      // 通过连续性校验，两个都不为断点
      previous_set->set_type = ERR_NO_ERROR;
      set->set_type = ERR_NO_ERROR;

      // 是否为断点已经确定，重置previous_set
      previous_set = NULL;
      return ERR_NO_ERROR;
    } else {
      // 不通过连续性校验，两个都为断点
      previous_set->set_type = ERR_BROKEN_POINT;
      set->set_type = ERR_BROKEN_POINT;

      // 是否为断点已经确定，重置previous_set
      previous_set = NULL;
      return ERR_BROKEN_POINT;
    }
  } else {
    // 如果Epoch变大，且断点后为FORMAT_DESCRIPTION_EVENT事件则为RDP切换
    if (before->trans && after->trans && before->epoch < after->epoch) {
      const ::rdp::messages::Event &after_event = after->trans->events(0);
      if (after_event.event_type() == FORMAT_DESCRIPTION_EVENT) {
        previous_set = set;
        set->set_type = ERR_NOT_CONFIRM;
        return ERR_NOT_CONFIRM;
      }
    }
    return ERR_BROKEN_POINT;
  }
}

static void ErrOffsetPrint(ErrOffset *err) {
  RDP_LOG_INFO << "Offset:[" << err->begin_offset << ":" << err->end_offset << "]";

  RDP_LOG_INFO << "GTID: " << err->trans->gtid() << ", Epoch: " << err->epoch
               << ", Transaction Seq No: " << err->trans->seq()
               << ", Binlog File Name: " << err->trans->binlog_file_name()
               << ", Position: " << err->trans->position()
               << ", Next Binlog File Name: " << err->trans->next_binlog_file_name()
               << ", Next Position: " << err->trans->next_position();

  int event_list_size = err->trans->events_size();
  RDP_LOG_INFO << "Event List Size: " << event_list_size;

  for (int i = 0; i < event_list_size; ++i) {
    const ::rdp::messages::Event &one_event = err->trans->events(i);
    RDP_LOG_INFO << "Index: " << i << ", Event Type: " << one_event.event_type()
                 << ", Binlog File Name: " << one_event.binlog_file_name()
                 << ", Position: " << one_event.position()
                 << ", Next Binlog Position: " << one_event.next_position();
  }
}

static json_t *ErrOffsetToJson(ErrOffset *err) {
  json_t *json = json_object();

  char buffer[32] = {0x0};
  size_t buffer_str_len = 0;

  buffer_str_len = snprintf(buffer, sizeof(buffer) - 1, "%" PRIu64, err->begin_offset);
  json_object_set_new(json, "begin_offset", json_stringn(buffer, buffer_str_len));

  buffer_str_len = snprintf(buffer, sizeof(buffer) - 1, "%" PRIu64, err->end_offset);
  json_object_set_new(json, "end_offset", json_stringn(buffer, buffer_str_len));

  json_object_set_new(json, "GTID", json_string(err->trans->gtid().c_str()));

  buffer_str_len = snprintf(buffer, sizeof(buffer) - 1, "%" PRIu64, err->epoch);
  json_object_set_new(json, "epoch", json_stringn(buffer, buffer_str_len));

  buffer_str_len = snprintf(buffer, sizeof(buffer) - 1, "%" PRId32, err->trans->seq());
  json_object_set_new(json, "trans_seqno", json_stringn(buffer, buffer_str_len));

  return json;
}

static void ErrOffsetSetPrint(ErrOffsetSet *set) {
  ErrOffset *before = &(set->before);
  ErrOffset *after = &(set->after);
  switch (set->type) {
    case EventErr:
      RDP_LOG_INFO << "Event List Broken Offset:[" << before->begin_offset << ":"
                   << before->end_offset << "]";
      ErrOffsetPrint(before);
      break;

    case TransErr:
      RDP_LOG_INFO << "Transaction Broken Offset:[" << before->end_offset << ":"
                   << after->begin_offset << "]";
      RDP_LOG_INFO << "Before Broken Point";
      ErrOffsetPrint(before);

      RDP_LOG_INFO << "After Broken Point";
      ErrOffsetPrint(after);
      break;

    default:
      assert(0);
  }
}

static json_t *ErrOffsetSetToJson(ErrOffsetSet *set) {
  json_t *json = NULL;

  ErrOffset *before = &(set->before);
  ErrOffset *after = &(set->after);
  int rc = 0;
  switch (set->type) {
    case EventErr:
      return ErrOffsetToJson(before);

    case TransErr:
      json = json_object();
      if (0 != (rc = json_object_set_new(json, "pre", ErrOffsetToJson(before)))) {
        RDP_LOG_ERROR << "Column json_array_append_new failed:" << rc;
        goto err;
      }

      if (0 != (rc = json_object_set_new(json, "next", ErrOffsetToJson(after)))) {
        RDP_LOG_ERROR << "Column json_array_append_new failed:" << rc;
        goto err;
      }

      break;

    default:
      assert(0);
  }
  return json;

err:
  if (json) {
    json_decref(json);
    json = NULL;
  }

  return NULL;
}

void ErrPrint() {
  if (err_vec.size() == 0) {
    RDP_LOG_INFO << "No Broken Position!";
    return;
  }
  int broken_count = 0;
  ErrSetType err_set_type;
  for (ErrOffsetVector::iterator it = err_vec.begin(); it != err_vec.end(); ++it) {
    switch ((*it)->type) {
      case EventErr:
        broken_count++;
        ErrOffsetSetPrint(*it);
        break;

      case TransErr:
        //if (ERR_BROKEN_POINT == (err_set_type = (*it)->set_type)) {
          broken_count++;
          RDP_LOG_INFO << "Broken Point Index: " << broken_count;
          ErrOffsetSetPrint(*it);
        //}
        break;

      default:
        assert(0);
    }
  }
  RDP_LOG_INFO << "Total Broken Point: " << broken_count;
  // not delete err_vec, free err_vec memory in program exit
}

void AddErr(ErrType type, PackingMsg *before_msg, PackingMsg *after_msg) {
  ErrOffsetSet *set = new ErrOffsetSet();

  set->type = type;
  switch (type) {
    case EventErr:
      // events连续性错误只需保存到before, after不需要
      set->before.begin_offset = before_msg->begin_offset;
      set->before.end_offset = before_msg->end_offset;
      set->before.trans = before_msg->transaction;
      set->before.epoch = before_msg->epoch;
      set->type = type;
      // before_msg->transaction had save in err_vec
      before_msg->save_in_err_flag = true;
      RDP_LOG_ERROR << "Find a broken point at offset: " << before_msg->begin_offset;
      break;

    case TransErr:
      set->before.begin_offset = before_msg->begin_offset;
      set->before.end_offset = before_msg->end_offset;
      set->before.trans = before_msg->transaction;
      set->before.epoch = before_msg->epoch;

      set->after.begin_offset = after_msg->begin_offset;
      set->after.end_offset = after_msg->end_offset;
      set->after.trans = after_msg->transaction;
      set->after.epoch = after_msg->epoch;

      // before_msg->transaction had save in err_vec
      before_msg->save_in_err_flag = true;
      // after_msg->transaction had save in err_vec
      after_msg->save_in_err_flag = true;
      RDP_LOG_ERROR << "Find a broken point after offset: " << before_msg->begin_offset << ", and before offset: " << after_msg->begin_offset;
      break;

    default:
      assert(0);
  }

  err_vec.push_back(set);
}

static bool ErrJsonArrayAdd(json_t *array, ErrType type, int *broken_count) {
  json_t *erroffset_json = NULL;
  int rc = 0;

  // 确定是否为断点
  //for (ErrOffsetVector::iterator it = err_vec.begin(); it != err_vec.end(); ++it) {
  //  IsErrSet(*it);
  //}

  // 断点输出
  for (ErrOffsetVector::iterator it = err_vec.begin(); it != err_vec.end(); ++it) {
    if ((*it)->type == type /*&& ERR_BROKEN_POINT == (*it)->set_type*/) {
      (*broken_count)++;

      if (NULL == (erroffset_json = ErrOffsetSetToJson(*it))) {
        RDP_LOG_ERROR << "ErrOffset Set To Json Failt";
        goto err;
      }

      if (0 != (rc = json_array_append_new(array, erroffset_json))) {
        RDP_LOG_ERROR << "Column json_array_append_new failed:" << rc;
        goto err;
      }
      erroffset_json = NULL;
    }
  }
  return true;

err:
  if (erroffset_json) {
    json_decref(erroffset_json);
    erroffset_json = NULL;
  }
  return false;
}

bool ErrToJson(const string file, int64_t start_offset, int64_t stop_offset) {
  char offset_str[32] = {0x0};
  size_t offset_str_len = 0;
  int broken_count = 0;
  int rc = 0;

  json_t *json = json_object();

  offset_str_len = snprintf(offset_str, sizeof(offset_str) - 1, "%" PRIu64, start_offset);
  json_object_set_new(json, "start_offset", json_stringn(offset_str, offset_str_len));

  offset_str_len = snprintf(offset_str, sizeof(offset_str) - 1, "%" PRIu64, stop_offset);
  json_object_set_new(json, "stop_offset", json_stringn(offset_str, offset_str_len));

  json_t *trans_array = json_array();
  json_object_set_new(json, "trans_miss", trans_array);

  json_t *event_array = json_array();
  json_object_set_new(json, "event_miss", event_array);

  if (err_vec.size() == 0) {
    goto end;
  }

  if (!ErrJsonArrayAdd(trans_array, TransErr, &broken_count)) {
    RDP_LOG_ERROR << "Add Trans Miss Array Failt";
    goto err;
  }

  if (!ErrJsonArrayAdd(event_array, EventErr, &broken_count)) {
    RDP_LOG_ERROR << "Add Event Miss Array Failt";
    goto err;
  }

end:
  offset_str_len = snprintf(offset_str, sizeof(offset_str) - 1, "%d", broken_count);
  json_object_set_new(json, "broken_count", json_stringn(offset_str, offset_str_len));

  if (0 != (rc = json_dump_file(json, file.c_str(),
                                JSON_COMPACT | JSON_ESCAPE_SLASH | JSON_SORT_KEYS))) {
    RDP_LOG_ERROR << "Dump Json To File: " << file << " Failt";
    goto err;
  }

  if (json) {
    json_decref(json);
    json = NULL;
  }

  return true;

err:
  if (json) {
    json_decref(json);
    json = NULL;
  }

  return false;
}
