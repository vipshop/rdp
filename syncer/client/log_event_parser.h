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

#ifndef _LOG_EVENT_PARSER_H_
#define _LOG_EVENT_PARSER_H_

#include "syncer_consts.h"
#include "syncer_def.h"
#include "syncer_events.h"
#include "syncer_incl.h"
#include "syncer_main.h"
#include "rdp.pb.h"

namespace syncer {

class LogEventParser {
public:
  explicit LogEventParser(void);
  ~LogEventParser();

public:
  Exit_status Parse(const char *buf, uint32_t len, 
                    const char *file_name, bool is_ddl, 
                    PreHeader *pre_header, rdp::messages::Event *event,
                    string *gtid, 
                    string &next_binlog_name, uint64_t &next_binlog_pos,
                    int64_t &last_committed, int64_t &sequence_number);
  void clear_tablemap();

private:
  bool Init(void);
  void Close(void);

private:
  Format_description_log_event *fd_log_event_;
  table_mapping                 tablemap_;
  bool                          with_orig_buf_;
};

} // namespace syncer

#endif // _LOG_EVENT_PARSER_H_
