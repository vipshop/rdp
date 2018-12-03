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

#include "syncer_progress.h"

namespace syncer {

SyncedProgress::SyncedProgress(SlaveView *slave_view) : znode_cver_(0) {
  gtid_mgr_ = new MySQLGtidMgr();
  assert(gtid_mgr_);
  memset(&sid_, 0x0, sizeof(sid_));
  gno_ = 0;
  memset(master_uuid_, 0x0, sizeof(master_uuid_));
  slave_view_ = slave_view;
}

SyncedProgress::~SyncedProgress() {
  if (NULL != gtid_mgr_) {
    delete gtid_mgr_;
    gtid_mgr_ = NULL;
  }
}

// Get exist gtid set in binary format
int32_t SyncedProgress::GetGTIDSet(uint8_t *buf) {
  return gtid_mgr_->Encode(buf);
}

// Overwrite exist gtid set, len = 0 mean buf is null-terminated C string
bool SyncedProgress::SetGTIDSet(const uint8_t *buf, uint32_t len) {
  // Reset gtid_mgr_
  gtid_mgr_->Reset();

  // Fill with current gtidset in text format
  if (len > 0) {
    std::string ss((const char *)buf, len);
    return gtid_mgr_->AddText(ss.c_str());
  } else {
    return gtid_mgr_->AddText((const char *)buf);
  }
}

// Sync gtid set from vms (checkpoint + delta)
bool SyncedProgress::DownloadProgress(void) { return true; }

// Cache latest gtid encountered, since a full transaction is made up like:
// { GTID | ... | ... | XID }, so we push the gtid to gtidset after the boundary
// of a transaction. When syncer_slave reconnect to MySQL master, we issue dump
// requests with this gtidset. (except the one in cache!)
void SyncedProgress::CacheLatestGtid(const char *buf, uint32_t len) {
  assert(0 == gno_);
  const char *ptr = buf + kBinlogHeaderSize;
  ptr += 1;

  sid_ = *(rpl_sid *)ptr;
  ptr += 16;
  gno_ = *(rpl_gno *)ptr;
  ptr += 8;
  assert(ptr - buf < len);
}

void SyncedProgress::ResetLatestGtid()
{
  memset(&sid_, 0x0, sizeof(sid_));
  gno_ = 0;
}

// Indicate end of a transaction, in case of FD and rotate event,
// there is not a GTID event, so gno_ can be 0; For normal DML/DDL,
// We should have encounter a GTID event
bool SyncedProgress::Commit(void) {
  if (0 != gno_) {
    if (slave_view_->IsInited()) {
      // Need to wait slaves
      slave_view_->WaitGtid(GetLatestGtid());
    }
    if (gtid_mgr_->AddGtid(sid_, gno_)) {
      gno_ = 0;
      return true;
    } else {
      return false;
    }
  }
  return true;
}

// Debug purpose
void SyncedProgress::Dump(void) {
  std::string cur_gtidset = gtid_mgr_->Print();
  RDP_LOG_INFO << "gtid_executed: " << cur_gtidset.c_str();
}

// Debug purpose
void SyncedProgress::DumpLatestGtid(void) {
  char uuid_str[Uuid::TEXT_LENGTH + 1] = {0x0};
  sid_.to_string(uuid_str);
  RDP_LOG_INFO << "Last Gtid: (" << uuid_str << ":" << gno_ <<")";
}

std::string SyncedProgress::GetLatestGtid() {
  if (gno_ == 0) {
    return "";
  }
  char uuid_str[Uuid::TEXT_LENGTH + 1] = {0x0};
  sid_.to_string(uuid_str);
  char gtid_str[128] = {0};
  snprintf(gtid_str, sizeof(gtid_str), "%s:%llu", uuid_str, gno_);
  return gtid_str;
}

// Merge purged gtid_set to current set
bool SyncedProgress::Merge(const Gtid_set *set) {
  return gtid_mgr_->Merge(set);
}

// Get exist gtid set in text format
std::string SyncedProgress::GetGTIDSet(void) {
  std::string cur_gtidset = gtid_mgr_->ToString();
  return cur_gtidset;
}

}  // namespace syncer
