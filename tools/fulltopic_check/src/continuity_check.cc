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

#include "continuity_check.h"

#include <iostream>
#include <vector>

#include <glog/logging.h>
#include <rdp.pb.h>

#include "write_csv.h"
#include "broken_points.h"

using std::string;
using std::vector;

const char* get_type_str(uint32_t type)
{
  switch(type) {
    case START_EVENT_V3:  return "Start_v3";
    case STOP_EVENT:   return "Stop";
    case QUERY_EVENT:  return "Query";
    case ROTATE_EVENT: return "Rotate";
    case INTVAR_EVENT: return "Intvar";
    case LOAD_EVENT:   return "Load";
    case NEW_LOAD_EVENT:   return "New_load";
    case CREATE_FILE_EVENT: return "Create_file";
    case APPEND_BLOCK_EVENT: return "Append_block";
    case DELETE_FILE_EVENT: return "Delete_file";
    case EXEC_LOAD_EVENT: return "Exec_load";
    case RAND_EVENT: return "RAND";
    case XID_EVENT: return "Xid";
    case USER_VAR_EVENT: return "User var";
    case FORMAT_DESCRIPTION_EVENT: return "Format_desc";
    case TABLE_MAP_EVENT: return "Table_map";
    case PRE_GA_WRITE_ROWS_EVENT: return "Write_rows_event_old";
    case PRE_GA_UPDATE_ROWS_EVENT: return "Update_rows_event_old";
    case PRE_GA_DELETE_ROWS_EVENT: return "Delete_rows_event_old";
    case WRITE_ROWS_EVENT_V1: return "Write_rows_v1";
    case UPDATE_ROWS_EVENT_V1: return "Update_rows_v1";
    case DELETE_ROWS_EVENT_V1: return "Delete_rows_v1";
    case BEGIN_LOAD_QUERY_EVENT: return "Begin_load_query";
    case EXECUTE_LOAD_QUERY_EVENT: return "Execute_load_query";
    case INCIDENT_EVENT: return "Incident";
    case IGNORABLE_LOG_EVENT: return "Ignorable";
    case ROWS_QUERY_LOG_EVENT: return "Rows_query";
    case WRITE_ROWS_EVENT: return "Write_rows";
    case UPDATE_ROWS_EVENT: return "Update_rows";
    case DELETE_ROWS_EVENT: return "Delete_rows";
    case GTID_LOG_EVENT: return "Gtid";
    case ANONYMOUS_GTID_LOG_EVENT: return "Anonymous_Gtid";
    case PREVIOUS_GTIDS_LOG_EVENT: return "Previous_gtids";
    case HEARTBEAT_LOG_EVENT: return "Heartbeat";
    case TRANSACTION_CONTEXT_EVENT: return "Transaction_context";
    case VIEW_CHANGE_EVENT: return "View_change";
    case XA_PREPARE_LOG_EVENT: return "XA_prepare";
#ifdef MARIADB
    case BINLOG_CHECKPOINT_EVENT: return "Binlog_checkpoint";
    case GTID_EVENT: return "Gtid";
    case GTID_LIST_EVENT: return "Gtid_list";
#endif
    default: return "Unknown";                            /* impossible */
  }
}



struct MatchData {
  bool init;
  bool is_consistent;
  uint64_t position;
  uint64_t next_position;
  uint64_t begin_offset;
  uint64_t end_offset;
  std::string uuid;
  std::string binlog_file_name;
  std::string next_binlog_file_name;
  PackingMsg *pre_msg;
  MatchData() {
    init = false;
    pre_msg = NULL;
    begin_offset = 0;
    end_offset = 0;
    position = 0;
    next_position = 0;
  }
  ~MatchData() {
    if (pre_msg) {
      delete pre_msg;
      pre_msg = NULL;
    }
  }
};

static MatchData match_data;

static string mariadb_get_uuid(string gtid) {
  string uuid = "";
  if (!gtid.empty()) {
    size_t start_offset = gtid.find('-', 0);
    assert(start_offset != string::npos);
    size_t end_offset = gtid.find('-', start_offset+1);
    assert(end_offset != string::npos);
    uuid = gtid.substr(start_offset, end_offset);
  }
  return uuid;
}

static uint64_t mariadb_get_serial(string gtid) {
  uint64_t serial = 0;
  if (!gtid.empty()) {
    size_t colon_offset = gtid.rfind('-', gtid.length());
    assert(colon_offset != string::npos);
    string serialstr = gtid.substr(colon_offset + 1, -1);
    serial = atoll(serialstr.c_str());
  }
  return serial;
}

static string mysql_get_uuid(string gtid) {
  string uuid;
  if (!gtid.empty()) {
    size_t colon_offset = gtid.find(':', 0);
    assert(colon_offset != string::npos);
    uuid = gtid.substr(0, colon_offset);
  }
  return uuid;
}

static uint64_t mysql_get_serial(string gtid) {
  uint64_t serial = 0;
  if (!gtid.empty()) {
    size_t colon_offset = gtid.find(':', 0);
    assert(colon_offset != string::npos);
    string serialstr = gtid.substr(colon_offset + 1, -1);
    serial = atoll(serialstr.c_str());
  }
  return serial;
}

static string get_uuid(string gtid) {
  return gtid;
  //if (gtid.find('-', 0) != string::npos)
  //  return mariadb_get_uuid(gtid);
  //else
  //  return mysql_get_uuid(gtid);
}

static uint64_t get_serial(string gtid) {
  if (gtid.find('-', 0) != string::npos)
    return mariadb_get_serial(gtid);
  else
    return mysql_get_serial(gtid);
}

PackingMsg::PackingMsg(uint64_t begin, uint64_t end, uint64_t p_epoch,
                       ::rdp::messages::Transaction *trans) {
  begin_offset = begin;
  end_offset = end;
  epoch = p_epoch;
  transaction = trans;
  save_in_err_flag = false;
}

PackingMsg::PackingMsg() {
  begin_offset = 0;
  end_offset = 0;
  epoch = 0;
  transaction = NULL;
  save_in_err_flag = false;
}

PackingMsg::~PackingMsg() {
  if (!save_in_err_flag && transaction) {
    delete transaction;
  }
}

static void IsSerial(PackingMsg *packing_msg) {
  ::rdp::messages::Transaction *trans = packing_msg->transaction;

  // 校验transaction内部event的连续性
  for (int i = 0; i < trans->events_size() - 1; ++i) {
    const ::rdp::messages::Event &cur_one_event = trans->events(i);
    const ::rdp::messages::Event &next_one_event = trans->events(i + 1);

    if (cur_one_event.next_position() != next_one_event.position() &&
        cur_one_event.binlog_file_name() == next_one_event.binlog_file_name()) {
      // 内部连续错误打印, 使用缩进标记
      RDP_LOG_INFO << "transaction broken offset:[" << packing_msg->begin_offset
                << ":" << packing_msg->end_offset << "]"
                << " Gtid: " << trans->gtid()
                << ", Transaction Seq: " << trans->seq()
                << " event type: " << cur_one_event.event_type() << "-"
                << next_one_event.event_type()
                << " binlog_file : " << cur_one_event.binlog_file_name() << ":"
                << cur_one_event.position() << "-"
                << cur_one_event.next_position() << " - "
                << next_one_event.binlog_file_name() << ":"
                << next_one_event.position() << "-"
                << next_one_event.next_position();
      // 保存错误transaction
      AddErr(EventErr, packing_msg, NULL);
    }
  }

  const string &gtid_msg = trans->gtid();
  const string uuid_msg = get_uuid(gtid_msg);

  const string &binlog_file_name = trans->binlog_file_name();
  uint64_t position = trans->position();

  // transaction连续性验证
  if (binlog_file_name.compare(match_data.next_binlog_file_name) != 0 || position != match_data.next_position) {
    // 可能是MySQL切换，或者RDP切换, 或者数据丢失
    bool is_broken = true;
    if (match_data.pre_msg->epoch < packing_msg->epoch) {
      // 如何是RDP切换了, RDP重连会收到的gtid_list/previous_gtid/binlog_checkpoint事件,
      // 对于这种导致链条断裂的情况, 不认为是断点
      if (trans->gtid().empty()) {
        is_broken = false;
      }
    }
    if (is_broken) {
      match_data.uuid = uuid_msg;
      match_data.binlog_file_name = trans->binlog_file_name();
      match_data.next_binlog_file_name = trans->next_binlog_file_name();
      match_data.position = trans->position();
      match_data.next_position = trans->next_position();
      match_data.begin_offset = packing_msg->begin_offset;
      match_data.end_offset = packing_msg->end_offset;

      // 保存错误transaction
      AddErr(TransErr, match_data.pre_msg, packing_msg);

      // 需要更新pre_msg, 指向下一个消息
      if (match_data.pre_msg) {
        delete match_data.pre_msg;
        match_data.pre_msg = NULL;
      }
      match_data.pre_msg = packing_msg;
    }
  } else {
    // 如果连续性校验已经通过，说明数据已经是完整的
    if (position == 4) {
        RDP_LOG_INFO << "Switched to binlog  '" << trans->binlog_file_name() << "', at offset: " << packing_msg->begin_offset;
    }
    match_data.uuid = uuid_msg;
    match_data.binlog_file_name = trans->binlog_file_name();
    match_data.next_binlog_file_name = trans->next_binlog_file_name();
    match_data.position = trans->position();
    match_data.next_position = trans->next_position();
    match_data.begin_offset = packing_msg->begin_offset;
    match_data.end_offset = packing_msg->end_offset;

    // 需要更新pre_msg, 指向下一个消息
    if (match_data.pre_msg) {
      delete match_data.pre_msg;
      match_data.pre_msg = NULL;
    }
    match_data.pre_msg = packing_msg;

  }
}

void MatchSerial(PackingMsg *packing_msg) {
  // init match_data if meet first packing_msg after start program
  if (!match_data.init) {
    ::rdp::messages::Transaction *trans = packing_msg->transaction;

    match_data.uuid = get_uuid(trans->gtid());

    match_data.binlog_file_name = trans->binlog_file_name();
    match_data.next_binlog_file_name = trans->next_binlog_file_name();
    match_data.position = trans->position();
    match_data.next_position = trans->next_position();

    match_data.begin_offset = packing_msg->begin_offset;
    match_data.end_offset = packing_msg->end_offset;

    match_data.pre_msg = packing_msg;

    match_data.init = true;
  } else {
    // 校验packing_msg和match_data.pre_msg的连续性
    IsSerial(packing_msg);

  }
}
