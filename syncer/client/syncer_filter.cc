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

#include <jansson.h>
#include "syncer_filter.h"
#include "syncer_app.h"

using namespace std;
using namespace rdp_comm;

namespace syncer {
int SyncerFilter::Init(ZKProcess *zk_handle) {
  struct Stat stat;
  string included_str;
  AppConfig *app_conf = g_syncer_app->app_conf_;
  string zk_path = app_conf->GetValue<string>("filter.path", "");
  if (!zk_path.empty() && 
      !zk_handle->GetData(zk_path+"/included", included_str, &stat)) {
    RDP_LOG_WARN << "Zookeeper path not exists: " << zk_path+"/included";
  }

  if (!included_str.empty()) {
    json_error_t jerror;
    json_t *jobj = json_loads(included_str.c_str(), 0, &jerror);
    if (!jobj) {
      RDP_LOG_ERROR << "Parse included_str failed: " << jerror.text;
      return -1;
    }

    // Start to iterater every database
    const char *db;
    json_t *jtable;
    json_object_foreach(jobj, db, jtable) {
      if  (included_map_.find(db) == included_map_.end()) {
        // Meet the database name first time
        included_map_[db] = TableMap();
      }
      TableMap &tables = included_map_[db];

      // Start to iterater every table
      const char *table;
      json_t *jcolumns;
      json_object_foreach(jtable, table, jcolumns) {
        if (tables.find(table) == tables.end()) {
          // Meet the table name first time
          tables[table] = ColumnMap();
        }
        ColumnMap &columns = tables[table];

        int column_count = json_array_size(jcolumns);
        for (int i = 0; i < column_count; i++) {
          json_t *jcolumn = json_array_get(jcolumns, i);
          string column_name = json_string_value(jcolumn);
          columns[column_name] = 1;
        }

      } // foreach

    } // foreach

  }

  return 0;
}

bool SyncerFilter::IsIgnored(string db, string table) {
  if (db == "sys" || db == "information_schema" || 
      db == "mysql" || db == "performance_schema") {
    return true;
  }

  if (included_map_.empty()) {
    // Not configured, every things can not ignored
    return false;
  }

  std::string tmp_db = db;
  if (included_map_.find(tmp_db) == included_map_.end()) {
      tmp_db = "";
      if (included_map_.find(tmp_db) == included_map_.end()) {
          // Not found, need to ignore this database
          return true;
      }
  }

  TableMap &tables = included_map_[tmp_db];
  if (tables.empty()) {
    // Not configured, every table can not ignored
    return false;
  }
  if (tables.find(table) == tables.end()) {
    // Not found, need to ignore this table
    return true;
  }

  return false;
  
}

bool SyncerFilter::IsIgnored(string db, string table, string column) {
  if (db == "sys" || db == "information_schema" || 
      db == "mysql" || db == "performance_schema") {
    return true;
  }

  if (included_map_.empty()) {
    // Not configured, every things can not ignored
    return false;
  }

  std::string tmp_db = db;
  if (included_map_.find(tmp_db) == included_map_.end()) {
      tmp_db = "";
      if (included_map_.find(tmp_db) == included_map_.end()) {
          // Not found, need to ignore this database
          return true;
      }
  }

  TableMap &tables = included_map_[tmp_db];
  if (tables.empty()) {
    // Not configured, every table can not ignored
    return false;
  }
  if (tables.find(table) == tables.end()) {
    // Not found, need to ignore this table
    return true;
  }


  ColumnMap &columns = tables[table];
  if (columns.empty()) {
    // Not configured, every column can not ignored
    return false;
  }
  if (columns.find(column) == columns.end()) {
    // Not found, need to ignore this column
    return true;
  }

  return false;
  
}

}
