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

#include "trx_boundary_parser.h"

namespace syncer {

extern enum_binlog_checksum_alg g_checksum_alg;

#ifndef DBUG_OFF
/* Event parser state names */
static const char *event_parser_state_names[] = {"None", "GTID", "DDL", "DML",
                                                 "Error"};
#endif

/*
-----------------------------------------
Transaction_boundary_parser class methods
-----------------------------------------
*/

/**
Reset the transaction boundary parser.

This method initialize the boundary parser state.
*/
void Transaction_boundary_parser::reset() {
  DBUG_ENTER("Transaction_boundary_parser::reset");
  DBUG_PRINT("info", ("transaction boundary parser is changing state "
                      "from '%s' to '%s'",
                      event_parser_state_names[current_parser_state],
                      event_parser_state_names[EVENT_PARSER_NONE]));
  current_parser_state = EVENT_PARSER_NONE;
  encounter_ddl = false;
  DBUG_VOID_RETURN;
}

/**
Feed the transaction boundary parser with a Log_event of any type,
serialized into a char* buffer.

@param buf            Pointer to the event buffer.
@param length         The size of the event buffer.
@param fd_event       The description event of the master which logged
the event.
@param throw_warnings If the function should throw warning messages while
updating the boundary parser state.
While initializing the Relay_log_info the
relay log is scanned backwards and this could
generate false warnings. So, in this case, we
don't want to throw warnings.

@return  false if the transaction boundary parser accepted the event.
true if the transaction boundary parser didn't accepted the event.
*/
bool Transaction_boundary_parser::feed_event(const char *buf, size_t length,
                                             bool throw_warnings) {
  DBUG_ENTER("Transaction_boundary_parser::feed_event");
  enum_event_boundary_type event_boundary_type =
      get_event_boundary_type(buf, length, throw_warnings);
  DBUG_RETURN(update_state(event_boundary_type, throw_warnings));
}

/**
Get the boundary type for a given Log_event of any type,
serialized into a char* buffer, based on event parser logic.

@param buf               Pointer to the event buffer.
@param length            The size of the event buffer.
@param description_event The description event of the master which logged
the event.
@param throw_warnings    If the function should throw warnings getting the
event boundary type.
Please see comments on this at feed_event().

@return  the transaction boundary type of the event.
*/
Transaction_boundary_parser::enum_event_boundary_type
Transaction_boundary_parser::get_event_boundary_type(const char *buf,
                                                     size_t length,
                                                     bool throw_warnings) {
  DBUG_ENTER("Transaction_boundary_parser::get_event_boundary_type");

  Log_event_type event_type;
  enum_event_boundary_type boundary_type = EVENT_BOUNDARY_TYPE_ERROR;
  event_type = (Log_event_type)buf[EVENT_TYPE_OFFSET];
  DBUG_PRINT("info", ("trx boundary parser was fed with an event of type %s",
                      Log_event::get_type_str(event_type)));

  switch (event_type) {
  case binary_log::GTID_LOG_EVENT:
  case binary_log::ANONYMOUS_GTID_LOG_EVENT:
    boundary_type = EVENT_BOUNDARY_TYPE_GTID;
    break;

  /*
  There are four types of queries that we have to deal with: BEGIN, COMMIT,
  ROLLBACK and the rest.
  */
  case binary_log::QUERY_EVENT: {
    char *query = NULL;
    size_t qlen = 0;
    size_t start_offset = kBinlogHeaderSize;

    // Post-header
    start_offset += 4; // thread_id
    start_offset += 4; // query_exec_time
/* Get the query to let us check for BEGIN/COMMIT/ROLLBACK */
#if 1
    uint8_t schema_length = *(uint8_t *)(buf + start_offset);
    start_offset += 1;
    // uint16_t error_code = *(uint16_t *)(buf + start_offset);
    start_offset += 2;
    uint16_t status_vars_length = *(uint16_t *)(buf + start_offset);
    start_offset += 2;

    // Body for Query_event
    start_offset += status_vars_length;
    start_offset += (schema_length + 1);

    // char *db = (char *)(buf + kBinlogHeaderSize + 13 + status_length);
    query = (char *)(buf + start_offset);
    qlen = length - start_offset - (g_checksum_alg == binary_log::BINLOG_CHECKSUM_ALG_CRC32? 4: 0); // checksum on / off
    //RDP_LOG_INFO << query << ":" << qlen << ":"<<g_checksum_alg;
#else
    qlen = Query_log_event::get_query(buf, length, fd_event, &query);
#endif
    if (qlen == 0) {
      DBUG_ASSERT(query == NULL);
      boundary_type = EVENT_BOUNDARY_TYPE_ERROR;
      break;
    }

    //RDP_LOG_DBG << "Query string: (" << string(query, qlen) << ") / (" << qlen << ")"; 

    /*
    BEGIN is always the begin of a DML transaction.
    */
    if (!strncmp(query, "BEGIN", qlen) ||
        !strncmp(query, STRING_WITH_LEN("XA START")))
      boundary_type = EVENT_BOUNDARY_TYPE_BEGIN_TRX;

    /*
    COMMIT and ROLLBACK are always the end of a transaction.
    */
    else if (!strncmp(query, "COMMIT", qlen) ||
             (!native_strncasecmp(query, STRING_WITH_LEN("ROLLBACK")) &&
              native_strncasecmp(query, STRING_WITH_LEN("ROLLBACK TO "))))
      boundary_type = EVENT_BOUNDARY_TYPE_END_TRX;

    /*
    XA ROLLBACK is always the end of a XA transaction.
    */
    else if (!native_strncasecmp(query, STRING_WITH_LEN("XA ROLLBACK")))
      boundary_type = EVENT_BOUNDARY_TYPE_END_XA_TRX;

    /*
    If the query is not (BEGIN | XA START | COMMIT | [XA] ROLLBACK), it can
    be considered an ordinary statement.
    */
    else
      boundary_type = EVENT_BOUNDARY_TYPE_STATEMENT;

    break;
  }

  /*
  XID events are always the end of a transaction.
  */
  case binary_log::XID_EVENT:
    boundary_type = EVENT_BOUNDARY_TYPE_END_TRX;
    break;

  /*
  XA_prepare event ends XA-prepared group of events (prepared XA transaction).
  */
  case binary_log::XA_PREPARE_LOG_EVENT:
    boundary_type = EVENT_BOUNDARY_TYPE_END_TRX;
    break;

  /*
  Intvar, Rand and User_var events are always considered as pre-statements.
  */
  case binary_log::INTVAR_EVENT:
  case binary_log::RAND_EVENT:
  case binary_log::USER_VAR_EVENT:
    boundary_type = EVENT_BOUNDARY_TYPE_PRE_STATEMENT;
    break;

  /*
  The following event types are always considered as statements
  because they will always be wrapped between BEGIN/COMMIT.
  */
  case binary_log::EXECUTE_LOAD_QUERY_EVENT:
  case binary_log::TABLE_MAP_EVENT:
  case binary_log::APPEND_BLOCK_EVENT:
  case binary_log::BEGIN_LOAD_QUERY_EVENT:
  case binary_log::ROWS_QUERY_LOG_EVENT:
  case binary_log::WRITE_ROWS_EVENT:
  case binary_log::UPDATE_ROWS_EVENT:
  case binary_log::DELETE_ROWS_EVENT:
  case binary_log::WRITE_ROWS_EVENT_V1:
  case binary_log::UPDATE_ROWS_EVENT_V1:
  case binary_log::DELETE_ROWS_EVENT_V1:
  case binary_log::PRE_GA_WRITE_ROWS_EVENT:
  case binary_log::PRE_GA_DELETE_ROWS_EVENT:
  case binary_log::PRE_GA_UPDATE_ROWS_EVENT:
  case binary_log::VIEW_CHANGE_EVENT:
    boundary_type = EVENT_BOUNDARY_TYPE_STATEMENT;
    break;

  /*
  Rotate, Format_description and Heartbeat should be ignored.
  Also, any other kind of event not listed in the "cases" above
  will be ignored.
  */
  case binary_log::ROTATE_EVENT:
  case binary_log::FORMAT_DESCRIPTION_EVENT:
  case binary_log::HEARTBEAT_LOG_EVENT:
  case binary_log::PREVIOUS_GTIDS_LOG_EVENT:
  case binary_log::START_EVENT_V3:
  case binary_log::STOP_EVENT:
  case binary_log::LOAD_EVENT:
  case binary_log::SLAVE_EVENT:
  case binary_log::CREATE_FILE_EVENT:
  case binary_log::DELETE_FILE_EVENT:
  case binary_log::NEW_LOAD_EVENT:
  case binary_log::EXEC_LOAD_EVENT:
  case binary_log::INCIDENT_EVENT:
  case binary_log::TRANSACTION_CONTEXT_EVENT:
    boundary_type = EVENT_BOUNDARY_TYPE_IGNORE;
    break;

  /*
  If the event is none of above supported event types, this is probably
  an event type unsupported by this server version. So, we must check if
  this event is ignorable or not.
  */
  default:
    if (uint2korr(buf + FLAGS_OFFSET) & LOG_EVENT_IGNORABLE_F)
      boundary_type = EVENT_BOUNDARY_TYPE_IGNORE;
    else {
      boundary_type = EVENT_BOUNDARY_TYPE_ERROR;
      if (throw_warnings)
        RDP_LOG_ERROR << "Unsupported non-ignorable event fed into the event stream";
    }
  } /* End of switch(event_type) */

  // end:
  DBUG_RETURN(boundary_type);
}

/**
Update the boundary parser state based on a given boundary type.

@param event_boundary_type The event boundary type of the event used to
fed the boundary parser.
@param throw_warnings      If the function should throw warnings while
updating the boundary parser state.
Please see comments on this at feed_event().

@return  false State updated successfully.
true  There was an error updating the state.
*/
bool Transaction_boundary_parser::update_state(
    enum_event_boundary_type event_boundary_type, bool throw_warnings) {
  DBUG_ENTER("Transaction_boundary_parser::update_state");

  enum_event_parser_state new_parser_state = EVENT_PARSER_NONE;

  bool error = false;

  switch (event_boundary_type) {
  /*
  GTIDs are always the start of a transaction stream.
  */
  case EVENT_BOUNDARY_TYPE_GTID:
    /* In any case, we will update the state to GTID */
    new_parser_state = EVENT_PARSER_GTID;
    /* The following switch is mostly to differentiate the warning messages */
    switch (current_parser_state) {
    case EVENT_PARSER_GTID:
    case EVENT_PARSER_DDL:
    case EVENT_PARSER_DML:
      if (throw_warnings) {
        char buff[256];
        snprintf(buff, sizeof(buff), "GTID_LOG_EVENT or ANONYMOUS_GTID_LOG_EVENT "
                "is not expected in an event stream %s.",
                current_parser_state == EVENT_PARSER_GTID
                    ? "after a GTID_LOG_EVENT or an ANONYMOUS_GTID_LOG_EVENT"
                    : current_parser_state == EVENT_PARSER_DDL
                          ? "in the middle of a DDL"
                          : "in the middle of a DML"); /* EVENT_PARSER_DML */
        RDP_LOG_ERROR << buff;
      }
      error = true;
      break;
    case EVENT_PARSER_ERROR: /* we probably threw a warning before */
      error = true;
    /* FALL THROUGH */
    case EVENT_PARSER_NONE:
      break;
    }
    break;

  /*
  There are four types of queries that we have to deal with: BEGIN, COMMIT,
  ROLLBACK and the rest.
  */
  case EVENT_BOUNDARY_TYPE_BEGIN_TRX:
    /* In any case, we will update the state to DML */
    new_parser_state = EVENT_PARSER_DML;
    /* The following switch is mostly to differentiate the warning messages */
    switch (current_parser_state) {
    case EVENT_PARSER_DDL:
    case EVENT_PARSER_DML:
      if (throw_warnings)
        RDP_LOG_ERROR << "QUERY(BEGIN) is not expected in an event stream "
                "in the middle of a " << (current_parser_state == EVENT_PARSER_DDL ? "DDL" : "DML");
      error = true;
      break;
    case EVENT_PARSER_ERROR: /* we probably threw a warning before */
      error = true;
    /* FALL THROUGH */
    case EVENT_PARSER_NONE:
    case EVENT_PARSER_GTID:
      break;
    }
    break;

  case EVENT_BOUNDARY_TYPE_END_TRX:
    /* In any case, we will update the state to NONE */
    new_parser_state = EVENT_PARSER_NONE;
    /* The following switch is mostly to differentiate the warning messages */
    switch (current_parser_state) {
    case EVENT_PARSER_NONE:
    case EVENT_PARSER_GTID:
    case EVENT_PARSER_DDL:
      if (throw_warnings) {
        char buff[256];
        snprintf(buff, sizeof(256), "QUERY(COMMIT or ROLLBACK) or "
                "XID_LOG_EVENT is not expected "
                "in an event stream %s.",
                current_parser_state == EVENT_PARSER_NONE
                    ? "outside a transaction"
                    : current_parser_state == EVENT_PARSER_GTID
                          ? "after a GTID_LOG_EVENT"
                          : "in the middle of a DDL"); /* EVENT_PARSER_DDL */
        RDP_LOG_ERROR << buff;

      }
      error = true;
      break;
    case EVENT_PARSER_ERROR: /* we probably threw a warning before */
      error = true;
    /* FALL THROUGH */
    case EVENT_PARSER_DML:
      break;
    }
    break;

  case EVENT_BOUNDARY_TYPE_END_XA_TRX:
    /* In any case, we will update the state to NONE */
    new_parser_state = EVENT_PARSER_NONE;
    /* The following switch is mostly to differentiate the warning messages */
    switch (current_parser_state) {
    case EVENT_PARSER_NONE:
    case EVENT_PARSER_DDL:
      if (throw_warnings)
        RDP_LOG_ERROR << "QUERY(XA ROLLBACK) is not expected in an event stream " << 
                (current_parser_state == EVENT_PARSER_NONE ? 
                 "outside a transaction" : "in the middle of a DDL"); /* EVENT_PARSER_DDL */
      error = true;
      break;
    case EVENT_PARSER_ERROR: /* we probably threw a warning before */
      error = true;
    /* FALL THROUGH */
    case EVENT_PARSER_DML:
    /* XA ROLLBACK can appear after a GTID event */
    case EVENT_PARSER_GTID:
      break;
    }
    break;

  case EVENT_BOUNDARY_TYPE_STATEMENT:
    switch (current_parser_state) {
    case EVENT_PARSER_NONE:
      new_parser_state = EVENT_PARSER_NONE;
      break;
    case EVENT_PARSER_GTID:
    case EVENT_PARSER_DDL:
      encounter_ddl = true;
      new_parser_state = EVENT_PARSER_NONE;
      break;
    case EVENT_PARSER_DML:
      new_parser_state = current_parser_state;
      break;
    case EVENT_PARSER_ERROR: /* we probably threw a warning before */
      error = true;
      break;
    }
    break;

  /*
  Intvar, Rand and User_var events might be inside of a transaction stream if
  any Intvar, Rand and User_var was fed before, if BEGIN was fed before or if
  GTID was fed before.
  In the case of no GTID, no BEGIN and no previous Intvar, Rand or User_var
  it will be considered the start of a transaction stream.
  */
  case EVENT_BOUNDARY_TYPE_PRE_STATEMENT:
    switch (current_parser_state) {
    case EVENT_PARSER_NONE:
    case EVENT_PARSER_GTID:
      new_parser_state = EVENT_PARSER_DDL;
      break;
    case EVENT_PARSER_DDL:
    case EVENT_PARSER_DML:
      new_parser_state = current_parser_state;
      break;
    case EVENT_PARSER_ERROR: /* we probably threw a warning before */
      error = true;
      break;
    }
    break;

  /*
  Rotate, Format_description and Heartbeat should be ignored.
  The rotate might be fake, like when the IO thread receives from dump thread
  Previous_gtid and Heartbeat events due to reconnection/auto positioning.
  */
  case EVENT_BOUNDARY_TYPE_IGNORE:
    new_parser_state = current_parser_state;
    break;

  case EVENT_BOUNDARY_TYPE_ERROR:
    error = true;
    new_parser_state = EVENT_PARSER_ERROR;
    break;
  }

  DBUG_PRINT("info", ("transaction boundary parser is changing state "
                      "from '%s' to '%s'",
                      event_parser_state_names[current_parser_state],
                      event_parser_state_names[new_parser_state]));
  current_parser_state = new_parser_state;

  DBUG_RETURN(error);
}

Transaction_boundary_parser g_trx_boundary_parser;

} // namespace syncer
