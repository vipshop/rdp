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

#ifndef __RDP_SCHEMA_H__
#define __RDP_SCHEMA_H__

#include <map>
#include <string>


#include <rdp-comm/metric_reporter.h>
#include "syncer_incl.h"
#include "syncer_main.h"
#include "syncer_conf.h"


namespace syncer {

// ColumnInfo contains infos about a column, such as name and type
class ColumnInfo {
 public:
  ColumnInfo() 
  : is_unsigned_(false) {}
  ColumnInfo(const std::string &name, const std::string &type, 
      const std::string &charset, const std::string &key) {
    name_ = name;
    type_ = type;
    character_set_ = charset;
    key_ = key;
    is_unsigned_ = false;
  }
  ~ColumnInfo() {}


  bool operator==(const ColumnInfo& rhs) const {
    return name_ == rhs.name_ && type_ == rhs.type_ 
      && character_set_ == rhs.character_set_ && key_==rhs.key_;
  }

  const ColumnInfo& operator=(const ColumnInfo& rhs) {
    name_ = rhs.name_;
    type_ = rhs.type_;
    character_set_ = rhs.character_set_;
    key_= rhs.key_;
    is_unsigned_ = rhs.is_unsigned_;

    return *this;
  }

  std::string name_;
  std::string type_;
  std::string character_set_;

  // 索引类型
  std::string key_;

  bool is_unsigned_;
};


// TableInfo contains infos about a table, such as db_name, 
// table_name and all columns
typedef std::map<size_t,ColumnInfo> ColumnMap;
class TableInfo {
 public:
  TableInfo() : schema_id_(0), version_(0) {}
  ~TableInfo() {}

  bool operator==(const TableInfo& rhs) const {
    // Dont't care about schema_id and version
    return db_name_ == rhs.db_name_ && 
      table_name_ == rhs.table_name_ && column_map_ == rhs.column_map_;
  }

  const TableInfo& operator=(const TableInfo& rhs) {
    schema_id_ = rhs.schema_id_;
    db_name_ = rhs.db_name_;
    table_name_ = rhs.table_name_;
    version_ = rhs.version_;
    column_map_ = rhs.column_map_;
    return *this;
  }
  void PrintInfo();

  size_t schema_id_;
  std::string db_name_;
  std::string table_name_;
  size_t version_;
  ColumnMap column_map_;
};


// TableId is the key to present an table, it can distinguishs 
// between some 'database.table' like these: [x.y].[z] and [x].[y.z]
class TableId {
 public:
   bool operator<(const TableId& rhs) const {
     int rc = strcmp(db_name_.c_str(), rhs.db_name_.c_str());
     if (rc < 0) {
       return true;
     } else if (rc == 0) {
       return table_name_ < rhs.table_name_;
     } else {
       return false;
     }
   }
   bool operator==(const TableId& rhs) const {
     return db_name_ == rhs.db_name_&&
       table_name_ == rhs.table_name_;
   }
   std::string db_name_;
   std::string table_name_;

};


class MysqlTransactionGuard {
 public:
  MysqlTransactionGuard(MYSQL *db_conn)
    : db_conn_(db_conn), done_(true) {}
  ~MysqlTransactionGuard() {
    if (!done_) {
      Rollback();
    }
  }
  int Begin() {
    if (mysql_query(db_conn_, "BEGIN")) {
      return -1;
    }
    done_ = false;
    return 0;
  }

  int Commit() {
    if (mysql_query(db_conn_, "COMMIT")) {
      return -1;
    }
    done_ = true;
    return 0;
  }
  int Rollback() {
    if (mysql_query(db_conn_, "ROLLBACK")) {
      return -1;
    }
    done_ = true;
    return 0;
  }

 private:
  MYSQL *db_conn_;
  bool done_;
};


typedef std::map<TableId, TableInfo> TableMap; 
class SchemaManager {
 public:
  enum ErrorCode {
    EC_OK = 0,

    // Configure error
    EC_CNF_ERR,

    // Connect db failed
    EC_DB_ERR,

    // Execute sql failed
    EC_SQL_ERR,

    // Schema json data is inconsitent with current shcema
    EC_INCONSITENT_ERR,

    // Errors related to snapshot operations
    EC_SNAPSHOT_ERR,
    
    EC_UNKNOWN_ERR
  };

  SchemaManager();
  ~SchemaManager();

  int Init();
  int Init(AppConfig *conf);

  // Get the table info for the specificed table 
  int GetTableInfo(const std::string& db_name, const std::string& table_name, 
      TableInfo& table_info);

  // Trigger the Schema Store to update schema and ddl_gtid_executed,
  // it is a wrapper of DoExecuteDdl();
  int ExecuteDdl(const std::string& db_name, const std::string& query, 
      const std::string& gtid);

  ErrorCode GetError() { return error_code_; }

  // For unit test
  MYSQL *GetDbConnection() { return executer_db_conn_; }


 private:
  int InitMetaDbConnection();
  void CloseMetaDbConnection();
  int InitExecuterDbConnection();
  void CloseExecuterDbConnection();

  bool IsDdlStatement(const std::string& query);

  int IsDdlExecuted(const std::string& gtid, bool& executed);
  int UpdateDdlGtidExecuted(const std::string& gtid);

  std::string TrimSql(const std::string& query);

  int DoExecuteDdl(const std::string& db_name, const std::string& query, 
      const std::string& gtid);


  // Get table info from Schema Store
  int GetTableInfoFromSchemaStore(const std::string& db_name, const std::string& table_name, 
      TableInfo& table_info);
  // Check if the Schema Store is empty, because it 
  // is empty when RDP start to run at the first time 
  int IsSchemaStoreEmpty(bool &empty);
  // Notify Schema Store to update
  int NotifySchemaStore(const std::string& query, const std::string& gtid);
  int UpdateSchemaStore(const std::string& db_name, const std::string &table_name, 
      const TableInfo& table_info, const std::string& query="", const std::string& gtid="");
  // Parse json string into ColumnMap struct
  int ParseColumnMap(const std::string& json_str, ColumnMap& column_map);

  // Get table info from I_S
  int GetTableInfoFromIS(TableMap&);

  // Wait until human signal RDP to continue to run , 
  // after some errors happened
  int WaitForInterference();

  int TakeSchemaSnapshot();
  int ReplaySchemaSnapShot();
  int TryRecover();

  void SetError(ErrorCode ec) { error_code_ = ec; }

  int GetTableCaheCount();

  int InitMetrics();
  void UnInitMetrics();

  std::string QuoteString(const std::string &query);
  std::string QuoteEscapeString(const std::string &query);

  ErrorCode error_code_;

  // Table info from Schema Store, including schema_id and version
  TableMap table_map_cache_;
  pthread_rwlock_t cache_lock_;

  // Table info from I_S, without schema_id and version
  TableMap table_map_base_;

  std::string script_dir_;
  std::string log_dir_;

  // Connection info to connect with mysql
  std::string meta_db_host_;
  int meta_db_port_;
  std::string meta_db_user_;
  std::string meta_db_password_;
  std::string meta_db_database_;
  bool meta_db_connected_;
  MYSQL *meta_db_conn_;
  spinlock_t meta_db_lock_;

  std::string executer_db_host_;
  int executer_db_port_;
  std::string executer_db_user_;
  std::string executer_db_password_;
  std::string executer_db_database_;
  bool executer_db_connected_;
  MYSQL *executer_db_conn_;

  // For gtid set operations
  Sid_map sid_map_;  
  Gtid_set ddl_gtid_executed_;

  rdp_comm::AvgMetric* dml_rt_metric_;
  rdp_comm::CounterMetric* dml_count_metric_;
  rdp_comm::CounterMetric* dml_failed_count_metric_;

  rdp_comm::AvgMetric* ddl_rt_metric_;
  rdp_comm::CounterMetric* ddl_count_metric_;
  rdp_comm::CounterMetric* ddl_failed_count_metric_;

  rdp_comm::CounterMetric* table_count_metric_;

};
}

#endif
