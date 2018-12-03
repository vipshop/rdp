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

#ifndef _SYNCER_CONSTS_H_
#define _SYNCER_CONSTS_H_

#include <stdint.h>

namespace syncer {

// Binary file reading return status
enum FileReadStatus { READ_ERR = -1, READ_EOF = 0, READ_OK = 1 };

enum AllocatingBufferSize {
  kSocketIOBufferSize = (1 << 20),
  kEventLogBufferSize = (8 << 20),
  kGTIDPosBufferSize = (16 << 10),
  KLZCompressBufferSize = (32 << 20)
};

enum { kBinlogHeaderSize = 19 };

// Method to sync data to disk device
enum FileSyncMethod { SYNC_ASYNC = 0, SYNC_RANGE, SYNC_DATASYNC, SYNC_SYNC };

// Exit status for functions in this file
enum Exit_status {
  /** No error occurred and execution should continue. */
  OK_CONTINUE = 0,
  /** An error occurred and execution should stop. */
  ERROR_STOP,
  /** No error occurred but execution should stop. */
  OK_STOP
};

// Binlog pre-header and Format Description Event definition
// Debug propose https://dev.mysql.com/doc/internals/en/binlog-event-header.html
#pragma pack(push)
#pragma pack(1)
struct PreHeader {
  uint32_t timestamp;
  uint8_t event_type;
  uint32_t server_id;
  uint32_t event_size;
  uint32_t log_pos;
  uint16_t flag;
};

struct FDEEvent {
  uint16_t binlog_ver;
  char mysql_ver[50];
  uint32_t timestamp;
  uint8_t event_hsize;
};

#pragma pack(pop)

} // namespace syncer

#endif // _SYNCER_CONSTS_H_

