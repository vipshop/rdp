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

#ifndef _SYNCER_PROGRESS_H_
#define _SYNCER_PROGRESS_H_

#include <rdp-comm/logger.h>
#include "mysql_gtidmgr.h"
#include "syncer_conf.h"
#include "slave_view.h"
#include <rdp-comm/signal_handler.h>

namespace syncer {

class SyncedProgress {
 public:
  explicit SyncedProgress(SlaveView *);
  virtual ~SyncedProgress(void);

 public:
  // Sync gtid set from vms (checkpoint + delta)
  virtual bool DownloadProgress(void);

  // Get exist gtid set in binary format
  int32_t GetGTIDSet(uint8_t *buf);
  // Get exist gtid set in text format
  std::string GetGTIDSet(void);

  // Overwrite exist gtid set, len = 0 mean buf is null-terminated C string
  bool SetGTIDSet(const uint8_t *buf, uint32_t len = 0);

  // Cache latest gtid encountered, since a full transaction is made up like:
  // { GTID | ... | ... | XID }, so we push the gtid to gtidset after the
  // boundary of a transaction. When syncer_slave reconnect to MySQL master, we
  // issue dump requests with this gtidset. (except the one in cache!)
  void CacheLatestGtid(const char *buf, uint32_t len);
  void ResetLatestGtid();
  // Indicate end of a transaction
  bool Commit(void);

  // Debug purpose
  void Dump(void);

  // Debug purpose
  void DumpLatestGtid(void);

  // Merge purged gtid_set to current set
  bool Merge(const Gtid_set *set);

  inline uint64_t GetCVersion(void) { return znode_cver_; }

  inline void SetMasterUuid(const char *uuid_str) {
    memcpy(master_uuid_, uuid_str, sizeof(master_uuid_) - 1);
  }

  inline const char *GetMasterUuid() { return master_uuid_; }
  std::string GetLatestGtid();

 private:
  // Gtid ops
  MySQLGtidMgr *gtid_mgr_;
  // Current master uuid
  char master_uuid_[37];
  // cversion of current znode
  uint64_t znode_cver_;

 private:
  // Latest gtid cached
  rpl_sid sid_;
  rpl_gno gno_;
  SlaveView *slave_view_;
};

}  // namespace syncer

#endif  // _SYNCER_PROGRESS_H_
