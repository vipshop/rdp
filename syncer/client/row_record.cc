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
#include "row_record.h"
#include "syncer_app.h"

namespace syncer {

Row::Row(const Row &row){
  action_ = row.action_;
  null_bitmap_.Assign(row.null_bitmap_.block_, row.null_bitmap_.size_);
  old_null_bitmap_.Assign(row.old_null_bitmap_.block_, row.old_null_bitmap_.size_);

  column_ = row.column_;
  old_column_ = row.old_column_;
  databasename_ = row.databasename_;
  tablename_ = row.tablename_;
  temp_ = row.temp_;
}
int Row::Init(EnumAction action, string databasename, string tablename) {
  action_ = action;
  databasename_ = databasename;
  tablename_ = tablename;
  return 0;
}

int Row::PushBack(uint32_t column_idx, char *buf, bool old) {
  if (old) {
    old_column_[column_idx] = buf;
  } else {
    column_[column_idx] = buf;
  }
  return 0;
}

int Row::PushBack(uint32_t column_idx, string buf, bool old) {
  if (old) {
    old_column_[column_idx] = buf;
  } else {
    column_[column_idx] = buf;
  }
  return 0;
}

int Row::PushBack(uint32_t column_idx, char *buf, uint32_t len, bool old) {
  temp_.assign(buf, len);
  if (old) {
    old_column_[column_idx] = temp_;
  } else {
    column_[column_idx] = temp_;
  }

  return 0;
}

string &Row::GetColumnByIndex(uint32_t index, bool old) {
  assert(index < column_.size());
  if (old)
    return old_column_[index];
  else
    return column_[index];
}

// Debug purpose
struct MsgPacker {
  std::string msg;
  MsgPacker(void) : msg("") {}

  MsgPacker *Append(std::string s) {
    msg.append(s);
    msg.append(" ");
    return this;
  }

  MsgPacker *Append(std::string key, std::string val) {
    msg += key;
    msg += " : \"";
    msg += val;
    msg += "\"";
    return this;
  }

  MsgPacker *Append(std::string key, size_t n) {
    char tmp[32];
    snprintf(tmp, sizeof(tmp), "%zu", n);
    msg += key;
    msg += " : ";
    msg += tmp;
    return this;
  }
};

const std::string OpsTypes[] = {"Insert", "Update", "Delete"};

// Debug print
int Row::Print() {
  TableInfo table_info;
  if (g_syncer_app->schema_manager_->GetTableInfo(databasename_, tablename_, table_info)) {
    return -1;
  }
  ColumnMap& column_map = table_info.column_map_;

  MsgPacker mp;
  mp.Append("{ ");

  mp.Append("Catalog", "DML");
  mp.Append(",");
  mp.Append("Type", OpsTypes[action_]);
  mp.Append(",");
  mp.Append("Database", databasename_);
  mp.Append(",");
  mp.Append("Table", tablename_);
  mp.Append(",");
  mp.Append("SchemaId", table_info.schema_id_);
  mp.Append(",");
  mp.Append("OldColumnSize", old_column_.size());
  mp.Append(",");
  mp.Append("NewColumnSize", column_.size());
  mp.Append(",");

  if (!old_column_.empty()) {
    mp.Append("{ ");
    for (std::map<uint32_t, std::string>::iterator it = old_column_.begin(); it != old_column_.end(); it++){
      if (it != old_column_.begin())
        mp.Append(",");
      mp.Append(column_map[it->first+1].name_, it->second);
    }
    mp.Append(" }");
  }

  mp.Append("{ ");
  for (std::map<uint32_t, std::string>::iterator it = column_.begin(); it != column_.end(); it++){
    if (it != column_.begin())
      mp.Append(",");
    mp.Append(column_map[it->first+1].name_, it->second);
  }
  mp.Append(" }");

  mp.Append(" }");
  RDP_LOG_INFO << mp.msg.c_str();
  return 0;
}

} // namespace syncer
