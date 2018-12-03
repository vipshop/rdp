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
#include "binlog_parser.h"
#include "binlog_reader.h"
#include "syncer_app.h"

namespace syncer {

// A forever loop to read binlog file and encode to JSON
bool BinlogParser::ParseBinlogs(const char *file_name, long offset) {
  char next_fn[256];
  BinlogReader *br = NULL;
  uint8_t *ptr = NULL;
  (void)ptr;
  FileReadStatus rc = READ_ERR;
  uint32_t binlog_id = 0;

  // TODO: Invoke via condition like semphore
  AppConfig *conf = g_syncer_app->app_conf_;
  TimedWaiter flush_waiter(conf->GetValue<int>("flush.wait", 1));
  TimedWaiter eof_waiter(conf->GetValue<int>("eof.wait", 1));

  snprintf(next_fn, sizeof(next_fn), "%s", file_name);
  RDP_LOG_DBG << "Start parse binlog from "<< next_fn  << ":" << offset;

  // Enter parse loop infinitely, for each binlog file
  while (rdp_comm::signalhandler::IsRunning()) {
    br = new BinlogReader((const char *)next_fn, offset);

    // Wait until file header had been flushed
    while (rdp_comm::signalhandler::IsRunning()) {
      flush_waiter.Begin();
      if (br->WaitFlush()) {  // Request offset had not been flushed
        flush_waiter.End();
      } else {
        break;
      }
    }

    br->Begin();
    do {
      rc = br->Next();
      if (READ_EOF == rc) {  // Reading 'eof'
        eof_waiter.Begin();
        binlog_id = br->binlog_id_;
        snprintf(next_fn, sizeof(next_fn), "%s/%s%06d",
                 conf->GetValue<std::string>("binlog.dir").c_str(),
                 conf->GetValue<std::string>("binlog.basename").c_str(),
                 ++binlog_id);
        if (0 == access(next_fn, R_OK))
          break;
        else
          eof_waiter.End();
      } else if (READ_ERR == rc) {  // Reading 'error'
        // RDP_LOG_ERROR("BinlogReader::Next() failed.");
        goto quit_t;
      }

      // Read Log Event Ok
      ptr = br->Ptr();
      // TODO: parsing
    } while (rdp_comm::signalhandler::IsRunning());

    // Rotate to next file
    if (rdp_comm::signalhandler::IsRunning()) {
      snprintf(next_fn, sizeof(next_fn), "%s%06d",
               conf->GetValue<std::string>("binlog.basename").c_str(),
               binlog_id);
      offset = 4U;
      RDP_LOG_DBG << "rotate to: "<< next_fn;

      delete br;
      br = NULL;
    }
  }  // while (SigHandler::IsRunning())

quit_t:
  if (NULL != br) {
    delete br;
    br = NULL;
  }

  return (rc != READ_ERR);
}

}  // namespace syncer
