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

#include "mysql_gtidmgr.h"

namespace syncer {

MySQLGtidMgr::MySQLGtidMgr() { this->Init(); }
MySQLGtidMgr::~MySQLGtidMgr() { this->Clear(); }

void MySQLGtidMgr::Init(void) {
  sid_map_ = new Sid_map(NULL);
  assert(sid_map_);
  gtid_set_ = new Gtid_set(sid_map_);
  assert(gtid_set_);
}

void MySQLGtidMgr::Clear(void) {
  if (NULL != sid_map_) {
    delete sid_map_;
    sid_map_ = NULL;
  }

  if (NULL != gtid_set_) {
    delete gtid_set_;
    gtid_set_ = NULL;
  }
}

// Add to text format gtidset to current gtidset
bool MySQLGtidMgr::AddText(const char *str) {
  if (RETURN_STATUS_OK != gtid_set_->add_gtid_text(str)) {
    return false;
  }
  return true;
}

// Encode current gtidset to buf in binary format
// Retrun:
//  0  - Error
// >0  - Data length filled in buf
size_t MySQLGtidMgr::Encode(uint8_t *buf) {
  size_t len = 0;
  len = gtid_set_->get_encoded_length();
  gtid_set_->encode(buf);
  return len;
}

// Add a gtid into current gtidset
bool MySQLGtidMgr::AddGtid(const rpl_sid &sid, rpl_gno seq_no) {
  rpl_sidno sidno = sid_map_->add_sid(sid);
  if (RETURN_STATUS_OK != gtid_set_->ensure_sidno(sidno)) {
    return false;
  }
  gtid_set_->_add_gtid(sidno, seq_no);
  return true;
}

// Merge another gtidset to current gtidset
bool MySQLGtidMgr::Merge(const Gtid_set *set) {
  if (RETURN_STATUS_OK != gtid_set_->add_gtid_set(set)) {
    return false;
  }
  return true;
}

// Check whether current gtid set is a subset of another
bool MySQLGtidMgr::IsSubset(const Gtid_set *set) {
  bool rc = false;
  rc = gtid_set_->is_subset(set);
  return rc;
}

// Check whether current gtid set is a subset of another
bool MySQLGtidMgr::IsSubsetForSid(const Gtid_set *set, rpl_sidno super_sid,
                                  rpl_sidno subset_sid) {
  bool rc = false;
  rc = gtid_set_->is_subset_for_sid(set, super_sid, subset_sid);
  return rc;
}

// Dump current gtidset to text format
std::string MySQLGtidMgr::ToString(void) {
  char *tmp = NULL;
  std::string ss = "";

  gtid_set_->to_string(&tmp);

  ss = std::string(tmp);
  if (NULL != tmp) my_free(tmp);

  return ss;
}

// Dump current gtidset to text format
std::string MySQLGtidMgr::Print(void) {
  char *tmp = NULL;
  std::string ss = "";

  gtid_set_->to_string(&tmp);

  if (NULL != tmp) {
    // Replace '\n' to ' ' for printing
    char *iter = tmp;
    while (*iter) {
      if (*iter == '\n') *iter = ' ';
      ++iter;
    }
  }

  ss = std::string(tmp);
  if (NULL != tmp) my_free(tmp);

  return ss;
}

// Reset current gtidset to empty
void MySQLGtidMgr::Reset() {
  gtid_set_->clear();
}

bool MySQLGtidMgr::IsIncludeForStr(const char *str) {
  bool rc = false;
  Sid_map sid_map(NULL);
  Gtid_set gtid_set(&sid_map, NULL);
  if (RETURN_STATUS_OK != gtid_set.add_gtid_text(str)) {
    return false;
  }

  rc = gtid_set.is_subset(gtid_set_);
  return rc;
}

}  // namespace syncer
