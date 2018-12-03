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

#ifndef _SYNCER_SLAVE_H_
#define _SYNCER_SLAVE_H_

#include "syncer_app.h"
#include "syncer_incl.h"
#include "syncer_main.h"
#include "statistics_writer.h"
#include "rate_limiter.h"

namespace syncer {

// Talk to MySQL master before establish replication connection
class DumpPreparation {
public:
  explicit DumpPreparation(MYSQL *mysql);
  ~DumpPreparation();

public:
  bool DoPrepare(void);

private:
  bool VerifyMasterVersion(void);
  bool VerifyFencingToken(void);
  bool QueryMasterTimestamp(void);
  bool QueryMasterServerId(void);
  bool QueryMasterUUID(void);
  bool QueryMasterBinlogBasename(void);
  bool SetHeartbeatPeriod(void);
  bool SetRDPSessionUUID(void);
  bool RegisterToMaster(void);
  bool StartupSemiSyncDump(void);
  bool SetBinlogChecksum(void);

private:
  MYSQL *mysql_;
};

// MySQL slave wrapper
class MySQLSlave {
public:
  explicit MySQLSlave(void);
  ~MySQLSlave();

public:
  void SyncBinlogs(void);

private:
  // Callback implementation for semi-sync slave
  bool SlaveStartup(void);
  bool SlavePrepare(void);
  bool SlaveRequest(void);
  bool SlaveProcess(void);
  bool SlaveClearup(void);

private:
  MYSQL *mysql_;
  RateLimiter *rate_limiter_;
};

extern enum_binlog_checksum_alg g_checksum_alg;

} // namespace syncer

#endif // _SYNCER_SLAVE_H_
