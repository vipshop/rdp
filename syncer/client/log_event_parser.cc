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

#include <rdp-comm/signal_handler.h>
#include <rdp-comm/logger.h>

#include "log_event_parser.h"
#include "rows_log_event_ex.h"
#include "syncer_app.h"
#include "syncer_consts.h"

using namespace std;

namespace syncer {

LogEventParser::LogEventParser() { this->Init(); }
LogEventParser::~LogEventParser() { this->Close(); }

bool LogEventParser::Init() {
  fd_log_event_ = new Format_description_log_event(4, "5.7.17");
  assert(fd_log_event_);
  if (!fd_log_event_->is_valid()) {
    RDP_LOG_ERROR << "Invalid Format_description_log_event, header_is_valid:" << fd_log_event_->header_is_valid() << ", version_is_valid:" << fd_log_event_->version_is_valid();
    return false;
  }

  with_orig_buf_ = (bool)g_syncer_app->app_conf_->GetValue<uint32_t>("parsing.with_orig_buf", 0);
  RDP_LOG_DBG << "with_orig_buf is " << (with_orig_buf_ ? "true" : "false");

  return true;
}

void LogEventParser::Close() {
  if (fd_log_event_) {
    delete fd_log_event_;
    fd_log_event_ = NULL;
  }

  clear_tablemap();
}

void LogEventParser::clear_tablemap() { 
  tablemap_.clear_tables();
}

Exit_status LogEventParser::Parse(const char *buf, uint32_t len,
                                  const char *file_name, bool is_ddl,
                                  PreHeader *pre_header,
                                  rdp::messages::Event *event, string *gtid,
                                  string &next_binlog_name, uint64_t &next_binlog_pos,
                                  int64_t &last_committed, int64_t &sequence_number) {
  // PreHeader *ph = NULL;
  // ph = (PreHeader *)buf;
  Log_event_type type = binary_log::UNKNOWN_EVENT;

  type = (Log_event_type)buf[EVENT_TYPE_OFFSET];
  RDP_LOG_DBG << Log_event::get_type_str(type);

  EventBase *event_base = NULL;

  switch (type) {
    // TableMapEvent
    case binary_log::TABLE_MAP_EVENT: {

      // TODO: cache table map event till schema changed
      event_base = new TablemapEvent(type);
      assert(event_base);

      break;
    }

    // ROW format DML
    case binary_log::WRITE_ROWS_EVENT:
    case binary_log::UPDATE_ROWS_EVENT:
    case binary_log::DELETE_ROWS_EVENT: {
      event_base = new RowEvent(type);
      assert(event_base);

      break;
    }

    // Query event
    case binary_log::QUERY_EVENT: {
      event_base = new QueryEvent(type, is_ddl);
      assert(event_base);

      break;
    }

    // TODO: Load data events

    // Gtid event, start of an tranaction
    case binary_log::GTID_LOG_EVENT: {
      event_base = new GtidEvent(type);
      assert(event_base);
      break;
    }

    // Xid event, end of a DML transaction
    case binary_log::XID_EVENT: {
      event_base = new XidEvent(type);
      assert(event_base);
      break;
    }

    // ROTATE_EVENT event, end of a binlog file
    case binary_log::ROTATE_EVENT: {
      event_base = new RotateEvent(type);
      assert(event_base);
      break;
    }

    default:
      // TODO 如何处理未知event
      // RDP_LOG_WARN << "Unknown event type: " << type;
      event_base = new OtherEvent(type);
      assert(event_base);
      break;
  }

  event_base->Init((char *)buf, len, file_name,
                   fd_log_event_, &tablemap_, with_orig_buf_);

  if (event_base->Parse()) {
    goto err_t;
  }

  if (is_ddl && binary_log::QUERY_EVENT == type) {
    // DDL apply to schema store
    string ddl_db = ((QueryEvent *)event_base)->db_;
    string ddl_statement = string(((QueryEvent *)event_base)->query_str_,
                                  ((QueryEvent *)event_base)->query_len_);
    string ddl_gtid = *gtid;

    RDP_LOG_DBG << "DDL statement: (" << ddl_statement.c_str() << ")";
    if (g_syncer_app->app_conf_->GetValue<uint32_t>("ddl.enable", 1) &&
        !IsSystemSchema(ddl_db) && 
        g_syncer_app->schema_manager_->ExecuteDdl(ddl_db, ddl_statement,
                                                  ddl_gtid)) {
      goto err_t;
    }
  }

  if (event_base->Encode(event)) {
    goto err_t;
  }

  if (binary_log::GTID_LOG_EVENT == event_base->cmd_) {
    *gtid = ((GtidEvent *)event_base)->gtid_;
    last_committed = ((GtidEvent *)event_base)->last_committed_;
    sequence_number = ((GtidEvent *)event_base)->sequence_number_;    

  } else if (binary_log::ROTATE_EVENT == event_base->cmd_) {
    next_binlog_name = string(((RotateEvent *)event_base)->orig_event_->new_log_ident,
                              ((RotateEvent *)event_base)->orig_event_->ident_len);
    next_binlog_pos  = ((RotateEvent *)event_base)->orig_event_->pos;
  } else {
  }


  if (event_base) {
    delete event_base;
    event_base = NULL;
  }

  return OK_CONTINUE;

err_t:
  if (event_base) {
    delete event_base;
    event_base = NULL;
  }
  return ERROR_STOP; 
}

}  // namespace syncer
