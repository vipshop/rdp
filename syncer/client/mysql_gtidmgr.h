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

#ifndef _MYSQL_GTIDMGR_H_
#define _MYSQL_GTIDMGR_H_

#include <rdp-comm/logger.h>
#include "syncer_incl.h"
#include "syncer_main.h"

namespace syncer {

class MySQLGtidMgr {
 public:
  explicit MySQLGtidMgr(void);
  ~MySQLGtidMgr();

 public:
  // Add to text format gtidset to current gtidset
  bool AddText(const char *str);
  // Encode current gtidset to buf in binary format
  // Retrun:
  //  0  - Error
  // >0  - Data length filled in buf
  size_t Encode(uint8_t *buf);
  // Add a gtid into current gtidset
  bool AddGtid(const rpl_sid &sid, rpl_gno seq_no);
  // Merge another gtidset to current gtidset
  bool Merge(const Gtid_set *set);
  // Check whether current gtid set is a subset of another
  bool IsSubset(const Gtid_set *set);
  // Check whether current gtid set is a subset of another
  bool IsSubsetForSid(const Gtid_set *set, rpl_sidno super_sid,
                      rpl_sidno subset_sid);

  // Dump current gtidset to text format
  std::string ToString(void);
  // Dump current gtidset to text format
  std::string Print(void);

  // Reset current gtidset to empty
  void Reset(void);
  // Check current gtid set is include a subset
  bool IsIncludeForStr(const char *str);

 private:
  void Init(void);
  void Clear(void);

 private:
  Sid_map *sid_map_;
  Gtid_set *gtid_set_;
};

}  // namespace syncer

#endif  // _MYSQL_GTIDMGR_H_
