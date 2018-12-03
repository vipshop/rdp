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

#ifndef __SYNCER_FILTER_H__
#define __SYNCER_FILTER_H__

#include <string>
#include <map>
#include <rdp-comm/zk_process.h>
#include <rdp-comm/zk_process.h>

namespace syncer {
  class SyncerFilter {
    public :
      int Init(rdp_comm::ZKProcess *zk_handle);
      bool IsIgnored(std::string db, std::string table);
      bool IsIgnored(std::string db, std::string table, std::string column);


    private:
      typedef std::map<string, int> ColumnMap;
      typedef std::map<string, ColumnMap> TableMap;
      
      std::map<string, TableMap> included_map_;
  };

}


#endif


