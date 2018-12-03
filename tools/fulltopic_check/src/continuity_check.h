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

#ifndef RDP_SRC_CONTINUITY_CHECK_H
#define RDP_SRC_CONTINUITY_CHECK_H

#include "rdp.pb.h"
#include "logger.h"

using syncer::g_logger;

/**
  Enumeration type for the different types of log events.
  */
enum Log_event_type {
  /**
    Every time you update this enum (when you add a type), you have to
    fix Format_description_event::Format_description_event().
    */
  UNKNOWN_EVENT = 0,
  START_EVENT_V3 = 1,
  QUERY_EVENT = 2,
  STOP_EVENT = 3,
  ROTATE_EVENT = 4,
  INTVAR_EVENT = 5,
  LOAD_EVENT = 6,
  SLAVE_EVENT = 7,
  CREATE_FILE_EVENT = 8,
  APPEND_BLOCK_EVENT = 9,
  EXEC_LOAD_EVENT = 10,
  DELETE_FILE_EVENT = 11,
  /**
    NEW_LOAD_EVENT is like LOAD_EVENT except that it has a longer
    sql_ex, allowing multibyte TERMINATED BY etc; both types share the
    same class (Load_event)
    */
  NEW_LOAD_EVENT = 12,
  RAND_EVENT = 13,
  USER_VAR_EVENT = 14,
  FORMAT_DESCRIPTION_EVENT = 15,
  XID_EVENT = 16,
  BEGIN_LOAD_QUERY_EVENT = 17,
  EXECUTE_LOAD_QUERY_EVENT = 18,
  TABLE_MAP_EVENT = 19,

  /**
    The PRE_GA event numbers were used for 5.1.0 to 5.1.15 and are
    therefore obsolete.
    */
  PRE_GA_WRITE_ROWS_EVENT = 20,
  PRE_GA_UPDATE_ROWS_EVENT = 21,
  PRE_GA_DELETE_ROWS_EVENT = 22,

  /**
    The V1 event numbers are used from 5.1.16 until mysql-trunk-xx
    */
  WRITE_ROWS_EVENT_V1 = 23,
  UPDATE_ROWS_EVENT_V1 = 24,
  DELETE_ROWS_EVENT_V1 = 25,

  /**
    Something out of the ordinary happened on the master
    */
  INCIDENT_EVENT = 26,

  /**
    Heartbeat event to be send by master at its idle time
    to ensure master's online status to slave
    */
  HEARTBEAT_LOG_EVENT = 27,

  /**
    In some situations, it is necessary to send over ignorable
    data to the slave: data that a slave can handle in case there
    is code for handling it, but which can be ignored if it is not
    recognized.
    */
  IGNORABLE_LOG_EVENT = 28,
  ROWS_QUERY_LOG_EVENT = 29,

  /** Version 2 of the Row events */
  WRITE_ROWS_EVENT = 30,
  UPDATE_ROWS_EVENT = 31,
  DELETE_ROWS_EVENT = 32,
  GTID_LOG_EVENT = 33,
  ANONYMOUS_GTID_LOG_EVENT = 34,
  PREVIOUS_GTIDS_LOG_EVENT = 35,
  TRANSACTION_CONTEXT_EVENT = 36,
  VIEW_CHANGE_EVENT = 37,

  /* Prepared XA transaction terminal event similar to Xid */
  XA_PREPARE_LOG_EVENT = 38,
  /*
    Binlog checkpoint event. Used for XA crash recovery on the master, not used
    in replication.
    A binlog checkpoint event specifies a binlog file such that XA crash
    recovery can start from that file - and it is guaranteed to find all XIDs
    that are prepared in storage engines but not yet committed.
  */
  BINLOG_CHECKPOINT_EVENT = 161,
  /*
    Gtid event. For global transaction ID, used to start a new event group,
    instead of the old BEGIN query event, and also to mark stand-alone
    events.
  */
  GTID_EVENT = 162,
  /*
    Gtid list event. Logged at the start of every binlog, to record the
    current replication state. This consists of the last GTID seen for
    each replication domain.
  */
  GTID_LIST_EVENT = 163,
  /**
    Add new events here - right above this comment!
    Existing events (except ENUM_END_EVENT) should never change their numbers
    */
  ENUM_END_EVENT /* end marker */
};

const char* get_type_str(uint32_t type);

struct PackingMsg {
  uint64_t begin_offset;
  uint64_t end_offset;
  ::rdp::messages::Transaction *transaction;
  uint64_t epoch;
  bool save_in_err_flag;

  PackingMsg(uint64_t begin, uint64_t end, uint64_t p_epoch,
             ::rdp::messages::Transaction *trans);

  PackingMsg();

  ~PackingMsg();
};

void MatchSerial(PackingMsg *packing_msg);

#endif  // RDP_SRC_CONTINUITY_CHECK_H
