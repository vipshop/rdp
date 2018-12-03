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

#include <glog/logging.h>
#include <jansson.h>
#include <boost/lexical_cast.hpp>
#include <boost/bind.hpp>
#include <boost/regex.hpp>
#include <rdp-comm/fiu_engine.h>

#include "schema_manager.h"
#include "syncer_app.h"
#include "util_sub_process.h"

using namespace std;
using rdp_comm::MetricReporter;
using rdp_comm::CounterMetric;
using rdp_comm::AvgMetric;
using rdp_comm::MeterMetric;
using rdp_comm::GetIntervalMs;


namespace syncer {

  SchemaManager::SchemaManager()
    : error_code_(EC_OK), meta_db_port_(0), meta_db_connected_(false), meta_db_conn_(NULL),
  executer_db_port_(0), executer_db_connected_(false), executer_db_conn_(NULL), 
      sid_map_(NULL), ddl_gtid_executed_(&sid_map_,NULL) {
    PthreadCall("pthread_rwlock_init", pthread_rwlock_init(&cache_lock_, NULL));
    dml_rt_metric_ = NULL;
    dml_count_metric_ = NULL;
    dml_failed_count_metric_ = NULL;
    ddl_rt_metric_ = NULL;
    ddl_count_metric_ = NULL;
    ddl_failed_count_metric_ = NULL;
    table_count_metric_ = NULL;
  }

  SchemaManager::~SchemaManager() {
    UnInitMetrics();
    CloseMetaDbConnection();
    CloseExecuterDbConnection();
  }

  int SchemaManager::GetTableCaheCount() {
    int ret;
    PthreadCall("pthread_rwlock_rdlock", pthread_rwlock_rdlock(&cache_lock_));
    ret = table_map_cache_.size();
    PthreadCall("pthread_rwlock_rdlock", pthread_rwlock_unlock(&cache_lock_));
    return ret;
  }

  int SchemaManager::InitMetrics() {
    // Define the metric for GetTableInfo rt
    dml_rt_metric_ = new AvgMetric("get_table_info_rt", 10, 20000);
    dml_count_metric_ = new CounterMetric("get_table_info_count", 10);
    dml_failed_count_metric_ = new CounterMetric("get_table_info_failed_count", 10);

    ddl_rt_metric_ = new AvgMetric("execute_ddl_rt", 10, 100);
    ddl_count_metric_ = new CounterMetric("execute_ddl_count", 10);
    ddl_failed_count_metric_ = new CounterMetric("execute_ddl_failed_count", 10);

    table_count_metric_ = new CounterMetric("table_cache_count", 10);
    table_count_metric_->Use(boost::bind(&SchemaManager::GetTableCaheCount, this));

    MetricReporter* metric_reporter = g_syncer_app->metric_reporter_;
    if (metric_reporter->RegistMetric(dml_rt_metric_)) {
      return -1;
    }
    if (metric_reporter->RegistMetric(dml_count_metric_)) {
      return -1;
    }
    if (metric_reporter->RegistMetric(dml_failed_count_metric_)) {
      return -1;
    }

    if (metric_reporter->RegistMetric(ddl_rt_metric_)) {
      return -1;
    }
    if (metric_reporter->RegistMetric(ddl_count_metric_)) {
      return -1;
    }
    if (metric_reporter->RegistMetric(ddl_failed_count_metric_)) {
      return -1;
    }
    
    if (metric_reporter->RegistMetric(table_count_metric_)) {
      return -1;
    }

    return 0;
  }

  void SchemaManager::UnInitMetrics() {
    MetricReporter* metric_reporter = g_syncer_app->metric_reporter_;
    if (dml_rt_metric_) {
      metric_reporter->UnRegistMetric(dml_rt_metric_);
      delete dml_rt_metric_;
      dml_rt_metric_ = NULL;
    }
    if (dml_count_metric_) {
      metric_reporter->UnRegistMetric(dml_count_metric_);
      delete dml_count_metric_;
      dml_count_metric_ = NULL;
    }
    if (dml_failed_count_metric_) {
      metric_reporter->UnRegistMetric(dml_failed_count_metric_);
      delete dml_failed_count_metric_;
      dml_failed_count_metric_ = NULL;
    }


    if (ddl_rt_metric_) {
      metric_reporter->UnRegistMetric(ddl_rt_metric_);
      delete ddl_rt_metric_;
      ddl_rt_metric_ = NULL;
    }
    if (ddl_count_metric_) {
      metric_reporter->UnRegistMetric(ddl_count_metric_);
      delete ddl_count_metric_;
      ddl_count_metric_ = NULL;
    }
    if (ddl_failed_count_metric_) {
      metric_reporter->UnRegistMetric(ddl_failed_count_metric_);
      delete ddl_failed_count_metric_;
      ddl_failed_count_metric_ = NULL;
    }

    if (table_count_metric_) {
      metric_reporter->UnRegistMetric(table_count_metric_);
      delete table_count_metric_;
      table_count_metric_ = NULL;
    }
  }
  
  int SchemaManager::Init(AppConfig *conf) {
    meta_db_host_ = conf->GetValue<string>("schema.meta.db.host");
    meta_db_port_ = conf->GetValue<int>("schema.meta.db.port");
    meta_db_user_ = conf->GetValue<string>("schema.meta.db.user");
    meta_db_password_ = conf->GetValue<string>("schema.meta.db.password", "");
    meta_db_database_ = conf->GetValue<string>("schema.meta.db.database");

    executer_db_host_ = conf->GetValue<string>("schema.executer.db.host");
    executer_db_port_ = conf->GetValue<int>("schema.executer.db.port");
    executer_db_user_ = conf->GetValue<string>("schema.executer.db.user");
    executer_db_password_ = conf->GetValue<string>("schema.executer.db.password", "");

    script_dir_ = conf->GetValue<string>("script.dir", "../scripts");
    log_dir_ = conf->GetValue<string>("log.dir");

    if (meta_db_host_.empty() || meta_db_port_<=0 || meta_db_user_.empty() || meta_db_database_.empty() ) {
      RDP_LOG_ERROR << "Exists some empty config items";
      return EC_CNF_ERR;
    }

    if (executer_db_host_.empty() || executer_db_port_<=0 || executer_db_user_.empty()) {
      RDP_LOG_ERROR << "Exists some empty config items";
      return EC_CNF_ERR;
    }

    if (log_dir_.empty()) {
      RDP_LOG_ERROR << "Exists some empty config items";
      return EC_CNF_ERR;
    }

    lock_guard guard(&meta_db_lock_);
    if (InitMetaDbConnection()) {
      // Connect meta db failed
      return -1;
    }
    if (InitExecuterDbConnection()) {
      // Connect executer db failed
      return -1;
    }

    // Try to recover the schema if needed
    if (TryRecover()) {
      return -1;
    }

    // Load the current table infos from I_S. 
    // After ddl executed, we load again from I_S, and
    // diff with table_map_base_ to find what table has be changed. 
    if (GetTableInfoFromIS(table_map_base_)) {
      return -1;
    }

    // Check if Schema Store, if yes, initialize it
    bool empty;
    if (IsSchemaStoreEmpty(empty)) {
      return -1;
    }
    if (empty) {
      RDP_LOG_INFO << "Schema store is empty, initializing it with table info from information_schema";
      MysqlTransactionGuard tg(meta_db_conn_);
      if (tg.Begin()) {
        return -1;
      }
      for (TableMap::iterator it = table_map_base_.begin();
          it != table_map_base_.end(); it++) {
        if (UpdateSchemaStore(it->first.db_name_, it->first.table_name_, it->second)) {
          return -1;
        }
      }
      if (tg.Commit()) {
        return -1;
      }
    }

    if (InitMetrics()) {
      return -1;
    }

    return 0;
  }

  int SchemaManager::Init() {
    AppConfig *app_conf = g_syncer_app->app_conf_;
    return Init(app_conf);
  }

  int SchemaManager::GetTableInfo(const std::string& db_name, const std::string& table_name, 
      TableInfo& table_info) {
    int err = 0;
    bool hit_the_cache = false;

    TableId key;
    key.db_name_ = db_name;
    key.table_name_ = table_name;

    PthreadCall("pthread_rwlock_rdlock", pthread_rwlock_rdlock(&cache_lock_));
    if (table_map_cache_.find(key) != table_map_cache_.end()) {
      // Hit the cache
      hit_the_cache = true;
      table_info = table_map_cache_[key];
      PthreadCall("pthread_rwlock_unlock", pthread_rwlock_unlock(&cache_lock_));
      goto end;
    }
    PthreadCall("pthread_rwlock_unlock", pthread_rwlock_unlock(&cache_lock_));
   
    // Get table info from db
    struct timeval begin_time;
    struct timeval end_time;
    gettimeofday(&begin_time, NULL);
    if (GetTableInfoFromSchemaStore(db_name, table_name, table_info)) {
      err = -1;
      goto end;
    }

    // Add to cache
    PthreadCall("pthread_rwlock_wrlock", pthread_rwlock_wrlock(&cache_lock_));
    table_map_cache_[key] = table_info;
    PthreadCall("pthread_rwlock_unlock", pthread_rwlock_unlock(&cache_lock_));

end:
    dml_count_metric_->AddValue(1);
    if (!hit_the_cache) {
      gettimeofday(&end_time, NULL);
      dml_rt_metric_->PushValue(GetIntervalMs(&begin_time, &end_time));
    }
    if (err) {
      dml_failed_count_metric_->AddValue(1);
    }
    return err;
  }

  int SchemaManager::ParseColumnMap(const string& json_str, ColumnMap& column_map) {
    json_error_t jerror;
    json_t *jobj = json_loads(json_str.c_str(), 0, &jerror);
    if (!jobj) {
      SetError(EC_UNKNOWN_ERR);
      return -1;
    }

    json_t *jcolumns = json_object_get(jobj, "columns");
    int isize = json_array_size(jcolumns);
    for (int i = 0; i < isize; i++) {
      ColumnInfo one_column;
      size_t position = 0;
      json_t *jcolumn = json_array_get(jcolumns, i);
      if (!jcolumns) {
        continue;
      }
      json_t *jname = json_object_get(jcolumn, "name");
      if (jname) {
        one_column.name_ = json_string_value(jname);
      }
      json_t *jtype = json_object_get(jcolumn, "type");
      if (jtype) {
        one_column.type_ = json_string_value(jtype);
        if (one_column.type_.find("unsigned") != string::npos || 
            one_column.type_.find("UNSIGNED") != string::npos ) {
          one_column.is_unsigned_ = true;
        }
      }
      json_t *jcharset = json_object_get(jcolumn, "character_set");
      if (jcharset) {
        one_column.character_set_ = json_string_value(jcharset);
      }

      json_t *jkey = json_object_get(jcolumn, "key");
      if (jkey) {
        one_column.key_ = json_string_value(jkey);
      }

      json_t *jposition = json_object_get(jcolumn, "position");
      if (jposition) {
        position = json_integer_value(jposition);
      }
      column_map[position] = one_column;
    }

    json_decref(jobj);

    return 0;
  }

  int SchemaManager::GetTableInfoFromSchemaStore(const string& db_name, const string& table_name, 
      TableInfo& table_info) {
    MYSQL_RES *res;
    MYSQL_ROW row;

    lock_guard guard(&meta_db_lock_);

    // TODO: Need to escape db_name and table_name
    string sql = "SELECT schema_id, db_name, table_name, version, schema_value " 
      " FROM tb_rdp_schema "
      " WHERE status=1"
      " AND db_name=" + QuoteString(db_name) +
      " AND table_name=" + QuoteString(table_name);


    RDP_LOG_DBG << "Executing sql: " << sql ;
    if (mysql_real_query(meta_db_conn_, sql.c_str(), sql.size())) {
      RDP_LOG_ERROR << "Execute sql failed: " << mysql_error(meta_db_conn_);
      RDP_LOG_ERROR << "Sql statement: " << sql;
      SetError(EC_SQL_ERR);
      return -1;
    }

    res = mysql_store_result(meta_db_conn_);
    if (!res) {
      RDP_LOG_ERROR << "Store mysql result failed: " << mysql_error(meta_db_conn_);
      RDP_LOG_ERROR << "Sql statement: " << sql;
      SetError(EC_SQL_ERR);
      return -1;
    }

    // The number of row must be exactly one
    if (mysql_num_rows(res) != 1) {
      RDP_LOG_ERROR << "result rows(" << mysql_num_rows(res) << ") is unexpected" ;
      RDP_LOG_ERROR << "Sql statement: " << sql;
      mysql_free_result(res);
      SetError(EC_INCONSITENT_ERR);
      return -1;
    }

    row = mysql_fetch_row(res);
    int num_fields = mysql_num_fields(res);
    MYSQL_FIELD *fields = mysql_fetch_fields(res);
    for (int i=0; i<num_fields; i++) {
      if (strcmp(fields[i].name, "schema_id") == 0) {
        table_info.schema_id_ = boost::lexical_cast<size_t>(row[i]);
      }
      if (strcmp(fields[i].name, "db_name") == 0) {
        table_info.db_name_ = static_cast<string>(row[i]);
      }
      if (strcmp(fields[i].name, "table_name") == 0) {
        table_info.table_name_ = static_cast<string>(row[i]);
      }
      if (strcmp(fields[i].name, "version") == 0) {
        table_info.version_ = boost::lexical_cast<size_t>(row[i]);
      }
      if (strcmp(fields[i].name, "schema_value") == 0) {
        string schema_value = static_cast<string>(row[i]);
        // Clear the table_info's column_map first
        table_info.column_map_.clear();
        // Schema_value is a json string, parse it into column map
        if (ParseColumnMap(schema_value, table_info.column_map_)) {
          RDP_LOG_ERROR << "parse schema_value failed";
          mysql_free_result(res);
          SetError(EC_INCONSITENT_ERR);
          return -1;
        }
      }

    }

    mysql_free_result(res);
    return 0;
  }

  int SchemaManager::IsSchemaStoreEmpty(bool &empty) {
    MYSQL_RES *res;
    MYSQL_ROW row;

    string sql = "SELECT count(*)" 
      " FROM tb_rdp_schema";

    RDP_LOG_DBG << "Executing sql: " << sql ;
    if (mysql_real_query(meta_db_conn_, sql.c_str(), sql.size())) {
      RDP_LOG_ERROR << "Execute sql failed: " << mysql_error(meta_db_conn_);
      RDP_LOG_ERROR << "Sql statement: " << sql;
      SetError(EC_SQL_ERR);
      return -1;
    }

    res = mysql_store_result(meta_db_conn_);
    if (!res) {
      RDP_LOG_ERROR << "Store mysql result failed: " << mysql_error(meta_db_conn_);
      RDP_LOG_ERROR << "Sql statement: " << sql;
      SetError(EC_SQL_ERR);
      return -1;
    }

    // The number of row must be exactly one
    if (mysql_num_rows(res) != 1) {
      RDP_LOG_ERROR << "result rows(" << mysql_num_rows(res) << ") is unexpected" ;
      RDP_LOG_ERROR << "Sql statement: " << sql;
      mysql_free_result(res);
      SetError(EC_INCONSITENT_ERR);
      return -1;
    }

    row = mysql_fetch_row(res);
    size_t count = boost::lexical_cast<size_t>(row[0]);
    empty = count == 0 ? true : false;

    mysql_free_result(res);
    return 0;
  }


  int SchemaManager::InitMetaDbConnection() {
    if (!meta_db_conn_) {
      meta_db_conn_ = mysql_init(NULL);
      if (!meta_db_conn_) {
        RDP_LOG_ERROR << "mysql_init failed";
        SetError(EC_UNKNOWN_ERR);
        return -1;
      }
      my_bool opt_value = true;

      // Reconnect if connection is lost when executing sql command
      if (mysql_options(meta_db_conn_, MYSQL_OPT_RECONNECT, &opt_value)) {
        RDP_LOG_ERROR << "mysql_options failed";
        mysql_close_free(meta_db_conn_);
        SetError(EC_UNKNOWN_ERR);
        return -1;
      }
    }

    if (!meta_db_connected_) {
      RDP_LOG_INFO << "Connecting meta db mysql conection...";
      if (!mysql_real_connect(meta_db_conn_, meta_db_host_.c_str(), meta_db_user_.c_str(), 
            meta_db_password_.c_str(), meta_db_database_.c_str(), meta_db_port_, NULL, 0)) {
        RDP_LOG_ERROR << "Connect mysql " << meta_db_host_ << ":" << meta_db_port_ << 
          " failed: " << mysql_error(meta_db_conn_);
        mysql_close_free(meta_db_conn_);
        SetError(EC_DB_ERR);
        return -1;
      }
      if (mysql_set_character_set(meta_db_conn_, "utf8")) {
        RDP_LOG_ERROR << "Set charset failed: " << mysql_error(meta_db_conn_);
        mysql_close_free(meta_db_conn_);
        SetError(EC_DB_ERR);
        return -1;
      }
      meta_db_connected_ = true;
    }

    return 0;
  }

  int SchemaManager::InitExecuterDbConnection() {
    if (!executer_db_conn_) {
      executer_db_conn_ = mysql_init(NULL);
      if (!executer_db_conn_) {
        RDP_LOG_ERROR << "mysql_init failed";
        SetError(EC_UNKNOWN_ERR);
        return -1;
      }
      my_bool opt_value = true;

      // Reconnect if connection is lost when executing sql command
      if (mysql_options(executer_db_conn_, MYSQL_OPT_RECONNECT, &opt_value)) {
        RDP_LOG_ERROR << "mysql_options failed";
        mysql_close(executer_db_conn_);
        SetError(EC_UNKNOWN_ERR);
        return -1;
      }
    }

    if (!executer_db_connected_) {
      RDP_LOG_INFO << "Connecting executer db mysql conection...";
      if (!mysql_real_connect(executer_db_conn_, executer_db_host_.c_str(), executer_db_user_.c_str(), 
            executer_db_password_.c_str(), NULL, executer_db_port_, NULL, 0)) {
        RDP_LOG_ERROR << "Connect mysql " << executer_db_host_ << ":" << executer_db_port_ << 
          " failed: " << mysql_error(executer_db_conn_);
        mysql_close_free(executer_db_conn_);
        SetError(EC_DB_ERR);
        return -1;
      }
      if (mysql_set_character_set(executer_db_conn_, "utf8")) {
        RDP_LOG_ERROR << "Set charset failed: " << mysql_error(executer_db_conn_);
        mysql_close_free(executer_db_conn_);
        SetError(EC_DB_ERR);
        return -1;
      }
      if (mysql_query(executer_db_conn_, "SET sql_mode=''")) {
        RDP_LOG_ERROR << "Set sql_mode failed: " << mysql_error(executer_db_conn_);
        mysql_close_free(executer_db_conn_);
        SetError(EC_DB_ERR);
        return -1;
      }
      if (mysql_query(executer_db_conn_, "SET foreign_key_checks=0")) {
        RDP_LOG_ERROR << "Set foreign_key_checks failed: " << mysql_error(executer_db_conn_);
        mysql_close_free(executer_db_conn_);
        SetError(EC_DB_ERR);
        return -1;
      }
      executer_db_connected_ = true;
    }

    return 0;
  }

  int SchemaManager::TakeSchemaSnapshot() {
    int status;
    string cmd = script_dir_ + "/take_schema_executer_snapshot.sh " + executer_db_host_ + " " + 
      boost::lexical_cast<string>(executer_db_port_) + " " + executer_db_user_ + " " + executer_db_password_ + " " +
      meta_db_database_.c_str();

    
    RDP_LOG_INFO << "Executing " << cmd;

    StringWriter sw1;
    StringWriter sw2;
    SubProcess process;
    process.SetCommand(script_dir_ + "/take_schema_executer_snapshot.sh", 5,
        executer_db_host_.c_str(), boost::lexical_cast<string>(executer_db_port_).c_str(), 
        executer_db_user_.c_str(), executer_db_password_.c_str(), meta_db_database_.c_str());
    process.SetStdout(&sw1);
    process.SetStderr(&sw2);

    if (!process.Create()) {
      RDP_LOG_ERROR << "Create sub process failed :" << cmd;
      SetError(EC_SNAPSHOT_ERR);
      return -1;
    }

    if (!process.Wait(&status)) {
      RDP_LOG_ERROR << "Wait sub process failed";
      SetError(EC_SNAPSHOT_ERR);
      return -1;
    }

    if (status) {
      RDP_LOG_ERROR << "Process exits with return code: " << status 
        << ", err: " << sw2.AsString();
      SetError(EC_SNAPSHOT_ERR);
      return -1;
    }

    // Store the sql content into mysql, not very graceful ^_^!
    string sql = "REPLACE INTO tb_schema_backup(id, schema_backup) VALUES(1,?)";
    MYSQL_STMT *stmt;    
    MYSQL_BIND bind[1];  

    stmt = mysql_stmt_init(meta_db_conn_);
    if (mysql_stmt_prepare(stmt, sql.c_str(), sql.size())) {
      RDP_LOG_ERROR << "mysql_stmt_prepare() failed: " << mysql_stmt_error(stmt);
      mysql_stmt_close(stmt);
      SetError(EC_SQL_ERR);
      return -1;
    }

    memset(bind, 0, sizeof(bind));
    bind[0].buffer_type= MYSQL_TYPE_BLOB;    
    bind[0].buffer= (void*)(sw1.AsString().c_str());
    bind[0].buffer_length= sw1.AsString().size();

    if (mysql_stmt_bind_param(stmt, bind)) {    
      RDP_LOG_ERROR << "mysql_stmt_bind_param() failed: " << mysql_stmt_error(stmt);
      mysql_stmt_close(stmt);
      SetError(EC_SQL_ERR);
      return -1;
    }    

    if (mysql_stmt_execute(stmt)) {    
      RDP_LOG_ERROR << "mysql_stmt_execute() failed: " << mysql_stmt_error(stmt);
      mysql_stmt_close(stmt);
      SetError(EC_SQL_ERR);
      return -1;
    }    
    mysql_stmt_close(stmt);

    return 0;
  }

  int SchemaManager::ReplaySchemaSnapShot() {
    int status;
    string cmd = script_dir_ + "/replay_schema_executer_snapshot.sh " + meta_db_host_ + " " + 
      boost::lexical_cast<string>(meta_db_port_) + " " + meta_db_user_ + " " + meta_db_password_ + " " + meta_db_database_ + " " +
      executer_db_host_ + " " + boost::lexical_cast<string>(executer_db_port_) + " " + executer_db_user_ + " " + executer_db_password_;

    RDP_LOG_INFO << "Executing " << cmd;

    StringWriter sw;
    SubProcess process;
    process.SetStderr(&sw);
    process.SetCommand(script_dir_ + "/replay_schema_executer_snapshot.sh", 9,
        meta_db_host_.c_str(), boost::lexical_cast<string>(meta_db_port_).c_str(), 
        meta_db_user_.c_str(), meta_db_password_.c_str(), meta_db_database_.c_str(),
        executer_db_host_.c_str(), boost::lexical_cast<string>(executer_db_port_).c_str(),
        executer_db_user_.c_str(), executer_db_password_.c_str());

    if (!process.Create()) {
      RDP_LOG_ERROR << "Create sub process failed :" << cmd;
      SetError(EC_SNAPSHOT_ERR);
      return -1;
    }

    if (!process.Wait(&status)) {
      RDP_LOG_ERROR << "Wait sub process failed";
      SetError(EC_SNAPSHOT_ERR);
      return -1;
    }

    if (status) {
      RDP_LOG_ERROR << "Process exits with return code: " << status 
        << ", err: " << sw.AsString();
      SetError(EC_SNAPSHOT_ERR);
      return -1;
    }
    return 0;
  }


  int SchemaManager::TryRecover() {
    // Clean executer mysqld
    int status;
    string cmd = script_dir_ + "/clean_schema_executer.sh " + executer_db_host_ + " " + 
      boost::lexical_cast<string>(executer_db_port_) + " " + executer_db_user_ + " " + executer_db_password_ + " " +
      meta_db_database_.c_str();

    
    RDP_LOG_INFO << "Executing " << cmd;

    StringWriter sw;
    SubProcess process;
    process.SetCommand(script_dir_ + "/clean_schema_executer.sh", 5,
        executer_db_host_.c_str(), boost::lexical_cast<string>(executer_db_port_).c_str(), 
        executer_db_user_.c_str(), executer_db_password_.c_str(), meta_db_database_.c_str());
    process.SetStderr(&sw);

    if (!process.Create()) {
      RDP_LOG_ERROR << "Create sub process failed :" << cmd;
      SetError(EC_SNAPSHOT_ERR);
      return -1;
    }

    if (!process.Wait(&status)) {
      RDP_LOG_ERROR << "Wait sub process failed";
      SetError(EC_SNAPSHOT_ERR);
      return -1;
    }

    if (status) {
      RDP_LOG_ERROR << "Process exits with return code: " << status 
        << ", err: " << sw.AsString();
      SetError(EC_SNAPSHOT_ERR);
      return -1;
    }


    // Replay snapshot
    RDP_LOG_WARN << "Recover schema executer db now";
    if (ReplaySchemaSnapShot()) {
      RDP_LOG_ERROR << "Replay snapshot failed";
      return -1;
    }
    return 0; 
  }

  int SchemaManager::IsDdlExecuted(const string& gtid, bool& executed) {
    // Already has meta_db_lock_ locked
    
    MYSQL_RES *res;
    MYSQL_ROW row;

    Gtid_set tmp_set(&sid_map_, NULL);
    if (tmp_set.add_gtid_text(gtid.c_str()) != RETURN_STATUS_OK) {
      return EC_INCONSITENT_ERR;
    }

    string sql = "SELECT ddl_gtid_executed FROM tb_ddl_gtid_executed";

    RDP_LOG_INFO << "Executing sql: " << sql ;
    if (mysql_real_query(meta_db_conn_, sql.c_str(), sql.size())) {
      RDP_LOG_ERROR << "Execute sql failed: " << mysql_error(meta_db_conn_);
      RDP_LOG_ERROR << "Sql statement: " << sql;
      SetError(EC_SQL_ERR);
      return -1;
    }

    res = mysql_store_result(meta_db_conn_);
    if (!res) {
      RDP_LOG_ERROR << "Store mysql result failed: " << mysql_error(meta_db_conn_);
      RDP_LOG_ERROR << "Sql statement: " << sql;
      SetError(EC_SQL_ERR);
      return -1;
    }

    // The number of row must be exactly one
    if (mysql_num_rows(res) != 1) {
      RDP_LOG_ERROR << "ddl_gtid_executed rows(" << mysql_num_rows(res) << ") is unexpected" ;
      RDP_LOG_ERROR << "Sql statement: " << sql;
      mysql_free_result(res);
      SetError(EC_INCONSITENT_ERR);
      return -1;
    }

    row = mysql_fetch_row(res);
    string gtid_text = static_cast<string>(row[0]);
    mysql_free_result(res);
    
    RDP_LOG_INFO << "The value of ddl_gtid_executed is: " << gtid_text;
    if (gtid_text[0] == '\0') {
      executed = false;
      return 0;
    }

    // Clear ddl_gtid_executed_
    ddl_gtid_executed_.clear();
    if (ddl_gtid_executed_.add_gtid_text(gtid_text.c_str()) != RETURN_STATUS_OK) {
      SetError(EC_INCONSITENT_ERR);
      return -1;
    }
    if (tmp_set.is_subset(&ddl_gtid_executed_)) {
      executed = true;
      return 0;
    }

    executed = false;
    return 0;
  }

  int SchemaManager::UpdateDdlGtidExecuted(const string& gtid) {
    Gtid_set tmp_set(&sid_map_, NULL);

    if (tmp_set.add_gtid_text(gtid.c_str()) != RETURN_STATUS_OK) {
      SetError(EC_UNKNOWN_ERR);
      return -1;
    }

    char *buf;
    ddl_gtid_executed_.add_gtid_set(&tmp_set);
    if ((ddl_gtid_executed_.to_string(&buf)) < 0) {
      my_free(buf);
      SetError(EC_UNKNOWN_ERR);
      return -1;
    }
    string gtid_set(buf);
    my_free(buf);

    string sql = "UPDATE tb_ddl_gtid_executed SET ddl_gtid_executed='"+ gtid_set + "'";

    RDP_LOG_INFO << "Executing sql: " << sql ;
    if (mysql_real_query(meta_db_conn_, sql.c_str(), sql.size())) {
      RDP_LOG_ERROR << "Execute sql failed: " << mysql_error(meta_db_conn_);
      SetError(EC_SQL_ERR);
      return -1;
    }

    return 0;
  }


  string SchemaManager::TrimSql(const string& query) {
    string trimed_query = query;

    boost::regex reg_single_comment1("--[^\\r\\n]*");
    boost::regex reg_single_comment2("^#[^\\r\\n]*");
    boost::regex reg_multi_comment("/\\*[\\w\\W]*?(?=\\*/)\\*/");
    boost::regex reg_newline("[\\r\\n]+");
    boost::regex reg_withspace("[\\s]+");
    boost::regex reg_withspace_begin("^[\\s]+");

    // 去除单行注释,还留下了换行
    trimed_query = boost::regex_replace(trimed_query, reg_single_comment1, "");
    trimed_query = boost::regex_replace(trimed_query, reg_single_comment2, "");
    // 去除块注释,用空格代替
    trimed_query = boost::regex_replace(trimed_query, reg_multi_comment, " ");
    // 去除换行, 用空格代替
    trimed_query = boost::regex_replace(trimed_query, reg_newline, " ");
    // 合并相邻的空白, 用单个空格代替
    trimed_query = boost::regex_replace(trimed_query, reg_withspace, " ");
    // 去除开头的空白
    trimed_query = boost::regex_replace(trimed_query, reg_withspace_begin, "");

    return trimed_query;

  }


  int SchemaManager::DoExecuteDdl(const string& db_name, const string& query, const string& gtid) {
    lock_guard guard(&meta_db_lock_);
    ErrorCode err = EC_OK;
    string sql;

    RDP_LOG_INFO << "Processing DDL("<< gtid << "): " << query;

    // Trim the comment/space in the query
    string trimed_query = TrimSql(query);
    RDP_LOG_INFO << "After trimed: " << trimed_query;

    if (!IsDdlStatement(trimed_query)) {
      RDP_LOG_INFO << "This query is not a real DDL";
      return 0;
    }

   
    // Load ddl_gtid_executed and see if the current DDL is executed 
    bool executed;
    if (IsDdlExecuted(gtid, executed)) {
      RDP_LOG_ERROR << "Check whether ddl has be executed failed";
      return -1;
    }

    if (executed) {
      RDP_LOG_WARN << "DDL(" << gtid << ") already executed before: " << query;
      return 0;
    }
    
    if (!db_name.empty()) {
      // 'CREATE DATABASE' don't need to execute 'USE'
      if (strncasecmp(trimed_query.c_str(), "CREATE DATABASE", strlen("CREATE DATABASE")) != 0 && 
          strncasecmp(trimed_query.c_str(), "DROP DATABASE", strlen("DROP DATABASE")) !=0 && 
          strncasecmp(trimed_query.c_str(), "CREATE SCHEMA", strlen("CREATE SCHEMA")) !=0 && 
          strncasecmp(trimed_query.c_str(), "DROP SCHEMA", strlen("DROP SCHEMA")) !=0 ) {
        sql = "USE `" + db_name +"`";
        RDP_LOG_INFO << "Executing sql: " << sql ;
        if (mysql_real_query(executer_db_conn_, sql.c_str(), sql.size())) {
          RDP_LOG_ERROR << "Execute sql failed: " << mysql_error(executer_db_conn_);
          err = EC_SQL_ERR;
          goto cancel;
        }
      }
    }
 

    // We need to rollback the DDL change if error occurs below


    sql = query;
    RDP_LOG_INFO << "Executing sql: " << sql ;
    if (mysql_real_query(executer_db_conn_, sql.c_str(), sql.size())) {
        // DDL may succeeds partially, need to rollback
        RDP_LOG_ERROR << "Execute sql failed: " << mysql_error(executer_db_conn_);
        RDP_LOG_ERROR << "Sql statement: " << sql;
        err = EC_SQL_ERR;
        goto rollback;
    }

    // Mock test to test the logic of rollback
    FIU_EXECUTE_IF("fail_after_ddl", {
      RDP_LOG_WARN << "In FIU fail_after_ddl point, rollback";
      if (UpdateDdlGtidExecuted(gtid)) {
        RDP_LOG_WARN << "UpdateDdlGtidExecuted failed";
      }
      err = EC_UNKNOWN_ERR;
      goto rollback;
    });
    FIU_EXECUTE_IF("crash_after_ddl", {
      RDP_LOG_WARN << "In FIU fail_after_ddl point, crash";
      if (UpdateDdlGtidExecuted(gtid)) {
        RDP_LOG_WARN << "UpdateDdlGtidExecuted failed";
      }
      abort();
    });
    
    {
      MysqlTransactionGuard tg(meta_db_conn_);
      if (tg.Begin()) {
        err = EC_SQL_ERR;
        goto rollback;
      }
      // Take a snapshot to backup the user schema
      if (TakeSchemaSnapshot()) {
        RDP_LOG_ERROR << "Take snapshot failed";
        err = GetError();
        goto rollback;
      }
      if (NotifySchemaStore(query,gtid)) {
        err = GetError();
        goto rollback;
      }
      if (UpdateDdlGtidExecuted(gtid)) {
        err = GetError();
        goto rollback;
      }
      if (tg.Commit()) {
        err = EC_SQL_ERR;
        goto rollback;
      }
    }


    // We can commit now
    goto commit;

rollback:
    // Rollback means "replay the snapshot which we have taken at last time we executed ddl"

    // Because we may be in chaos status(ie. we may be still in a transaction), 
    // so it is best to close the db connection, give a fresh status to next try
    CloseMetaDbConnection();
    InitMetaDbConnection();
    if (ReplaySchemaSnapShot()) {
      RDP_LOG_ERROR << "Replay snapshot failed";
      g_syncer_app->Alarm("Replay snapshot failed, rdp is hanging!");
      WaitForInterference();
    }
 
cancel:
    // May be Executer db has some problems, try to reconnect
    CloseExecuterDbConnection(); 
    InitExecuterDbConnection(); 
commit:

    if(err) {
      SetError(err);
      return -1;
    }

    return 0;
    
  }

  int SchemaManager::WaitForInterference() {
    // This is a infinite loop, because DDL must succeed eventually
    int i=0;
    string file_path = log_dir_+"/retry_ddl";
    while (true) {
      if (!access(file_path.c_str(), F_OK)) {
        RDP_LOG_WARN << "The retry_ddl file found";
        // Delete it
        unlink(file_path.c_str());
        break;
      }
      RDP_LOG_WARN << "The retry_ddl file not found, you can create it to tell me to continue";
      if (i%6 == 0) {
        // Trigger alarm every minute 
        g_syncer_app->Alarm("execute DDL failed, rdp is hanging!");
      }
      sleep(10);
      i++;
    }
    return 0;
  }


  int SchemaManager::ExecuteDdl(const string& db_name, const string& query, const string& gtid) {
    struct timeval begin_time;
    struct timeval end_time;
    gettimeofday(&begin_time, NULL);

    int try_times = 0;
    while(true) {
      try_times++ ;
      RDP_LOG_INFO << "DDL try times: " << try_times;
      if (!DoExecuteDdl(db_name, query, gtid)) {
        RDP_LOG_INFO << "Execute DDL succeed";
        // Succeed
        break;
      }
      RDP_LOG_ERROR << "Execute DDL failed";
      if (try_times >= 3) {
        try_times = 0;
        ddl_failed_count_metric_->AddValue(1);
        WaitForInterference();   
      }
      sleep(2);
    }

    ddl_count_metric_->AddValue(1);

    gettimeofday(&end_time, NULL);
    ddl_rt_metric_->PushValue(GetIntervalMs(&begin_time, &end_time));
    return 0;
  }

  void SchemaManager::CloseMetaDbConnection() {
    if (meta_db_conn_) {
      RDP_LOG_INFO << "Closing meta db mysql conection...";
      mysql_close_free(meta_db_conn_);
      meta_db_conn_ = NULL;
      meta_db_connected_ = false;
    }
  }

  void SchemaManager::CloseExecuterDbConnection() {
    if (executer_db_conn_) {
      RDP_LOG_INFO << "Closing executer db mysql conection...";
      mysql_close_free(executer_db_conn_);
      executer_db_conn_ = NULL;
      executer_db_connected_ = false;
    }
  }

  int SchemaManager::NotifySchemaStore(const string& query, const string& gtid) {
    // Already has meta_db_lock_ locked
    
    TableMap table_map_new;
    if (GetTableInfoFromIS(table_map_new)) {
      return -1;
    }

    // 比对两个map是否有变化
    if (table_map_base_ == table_map_new) {
      RDP_LOG_WARN << "Schema has no change, size is: " << table_map_base_.size() << endl;
    } else {
      RDP_LOG_INFO << "Schema has changed, size is: " << table_map_base_.size() << " --> " << table_map_new.size() << endl;
      for (TableMap::iterator iter = table_map_new.begin(); 
          iter != table_map_new.end(); iter++) {
        TableMap::iterator iter_old = table_map_base_.find(iter->first);
        if (iter_old != table_map_base_.end()) {
          if (iter->second == iter_old->second) {
            // 该表没有变化
            continue;
          } else {
            // 该表发生变化, 那么替换掉，并增加一条新的数据
            RDP_LOG_WARN << "Table is altered: " << iter->first.table_name_ << endl;
            if (UpdateSchemaStore(iter->first.db_name_, iter->first.table_name_, iter->second, query, gtid)) {
              return -1;
            }
            table_map_base_.erase(iter->first);
            table_map_base_[iter->first] = iter->second;
          }
        } else {
          // 该表是新增的，增加一条新的数据
          RDP_LOG_WARN << "Table is created: " << iter->first.table_name_ << endl;
          if (UpdateSchemaStore(iter->first.db_name_, iter->first.table_name_, iter->second, query, gtid)) {
            return -1;
          }
          table_map_base_[iter->first] = iter->second;
        }
      }

      // 令缓存失效, 无需加锁, 因为DDL是单线程执行
      table_map_cache_.clear();
    }
    return 0;
  }

  int SchemaManager::UpdateSchemaStore(const string& db_name, const string &table_name, 
      const TableInfo& table_info, const string& query, const string& gtid) {
    string sql;
    MYSQL_RES *res;
    MYSQL_ROW row;

    // Get max version for this table
    size_t max_version = 0;
    sql = 
      " SELECT version FROM tb_rdp_schema "
      " WHERE status=1 "
      " AND db_name=" + QuoteString(db_name) + " AND table_name=" + QuoteString(table_name) ;
    RDP_LOG_INFO << "Executing sql: " << sql ;
    if (mysql_real_query(meta_db_conn_, sql.c_str(), sql.size())) {
      RDP_LOG_ERROR << "Execute sql failed: " << mysql_error(meta_db_conn_);
      SetError(EC_SQL_ERR);
      return -1;
    }
    res = mysql_store_result(meta_db_conn_);
    if (!res) {
      RDP_LOG_ERROR << "Store mysql result failed: " << mysql_error(meta_db_conn_);
      SetError(EC_SQL_ERR);
      return -1;
    }
    if (mysql_num_rows(res) == 0) {
      // This is a new created table
      RDP_LOG_WARN << "This is a new created table: " << table_name ;
      max_version = 0;
    } else if (mysql_num_rows(res) == 1) {
      row = mysql_fetch_row(res);
      max_version = boost::lexical_cast<size_t>(row[0]);
    } else {
      SetError(EC_INCONSITENT_ERR);
      mysql_free_result(res);
      return -1;
    }
    mysql_free_result(res);

    max_version++;

    // Encode table info into json string
    string schema_value;
    if (table_info.column_map_.size() > 0) {
      json_t *json = json_object();

      json_t *array = json_array();
      for (ColumnMap::const_iterator iter = table_info.column_map_.begin();
          iter != table_info.column_map_.end(); iter++) {
        json_t *jcolumn = json_object();
        json_object_set_new(jcolumn, "name", json_string(iter->second.name_.c_str()));
        json_object_set_new(jcolumn, "type", json_string(iter->second.type_.c_str()));
        json_object_set_new(jcolumn, "character_set", json_string(iter->second.character_set_.c_str()));
        json_object_set_new(jcolumn, "key", json_string(iter->second.key_.c_str()));
        json_object_set_new(jcolumn, "position", json_integer(iter->first));
        int rc = json_array_append_new(array, jcolumn);
        if (rc != 0) {
          RDP_LOG_ERROR << "Column json_array_append_new failed:" << rc << endl;
        }
      }
      json_object_set_new(json, "columns", array);
      char *result = json_dumps(json, 0);
      schema_value = result;

      json_decref(json);
      free(result);

      RDP_LOG_DBG << "Columns json_dumps:" << schema_value << endl;
    }

    // Disable the previous record about this table
    size_t schema_id = 0;
    sql = 
      " UPDATE tb_rdp_schema "
      " SET status=0 "
      " WHERE db_name=" + QuoteString(db_name) + 
      " AND table_name=" + QuoteString(table_name) + 
      " AND version<" + boost::lexical_cast<string>(max_version) ;

    RDP_LOG_INFO << "Executing sql: " << sql ;
    if (mysql_real_query(meta_db_conn_, sql.c_str(), sql.size())) {
      RDP_LOG_ERROR << "Execute sql failed: " << mysql_error(meta_db_conn_);
      SetError(EC_SQL_ERR);
      return -1;
    }

    // Insert a new record
    sql =
      " INSERT INTO tb_rdp_schema "
      " SET db_name=" + QuoteString(db_name) + ","
      " table_name=" + QuoteString(table_name) + "," 
      " version=" + boost::lexical_cast<std::string>(max_version) + ","
      " schema_value=" + QuoteEscapeString(schema_value) + ","
      " statement=" + QuoteEscapeString(query) + ","
      " gtid=" + QuoteString(gtid) + ","
      " status=1";

    RDP_LOG_INFO << "Executing sql: " << sql ;
    if (mysql_real_query(meta_db_conn_, sql.c_str(), sql.size())) {
      RDP_LOG_ERROR << "Execute sql failed: " << mysql_error(meta_db_conn_);
      SetError(EC_SQL_ERR);
      return -1;
    }

    // Retrieve the schema_id
    sql = "SELECT last_insert_id() AS schema_id";
    RDP_LOG_INFO << "Executing sql: " << sql ;
    if (mysql_real_query(meta_db_conn_, sql.c_str(), sql.size())) {
      RDP_LOG_ERROR << "Execute sql failed: " << mysql_error(meta_db_conn_);
      SetError(EC_SQL_ERR);
      return -1;
    }
    res = mysql_store_result(meta_db_conn_);
    if (!res) {
      RDP_LOG_ERROR << "Store mysql result failed: " << mysql_error(meta_db_conn_);
      SetError(EC_SQL_ERR);
      return -1;
    }
    if (mysql_num_rows(res) != 1) {
      RDP_LOG_ERROR << "get last_insert_id() failed";
      SetError(EC_INCONSITENT_ERR);
      mysql_free_result(res);
      return -1;
    } else {
      row = mysql_fetch_row(res);
      schema_id = boost::lexical_cast<size_t>(row[0]);
      mysql_free_result(res);
    }

    RDP_LOG_DBG << "UpdateSchemaTable:" << db_name << "." << table_name
      << ", schema_id:" << schema_id << ", max_version:" << max_version
      << ", schema:" << schema_value << endl;

    return 0;
  }

  int SchemaManager::GetTableInfoFromIS(TableMap &table_map) {
    MYSQL_RES *res;
    MYSQL_ROW row;

    string sql = 
      " SELECT table_schema,table_name,ordinal_position,column_name,column_type,character_set_name,column_key "
      " FROM information_schema.columns "
      " WHERE table_schema NOT IN "
      " ('information_schema','mysql','performance_schema','sys')"
      " ORDER BY table_schema";

    RDP_LOG_INFO << "Executing sql: " << sql ;
    if (mysql_real_query(executer_db_conn_, sql.c_str(), sql.size())) {
      RDP_LOG_ERROR << "Execute sql failed: " << mysql_error(executer_db_conn_);
      SetError(EC_SQL_ERR);
      return -1;
    }

    res = mysql_store_result(executer_db_conn_);
    if (!res) {
      RDP_LOG_ERROR << "Store mysql result failed: " << mysql_error(executer_db_conn_);
      SetError(EC_SQL_ERR);
      return -1;
    }

    // The number of row must be exactly one
    if (mysql_num_rows(res) == 0) {
      RDP_LOG_WARN << "zero result rows is unexpected" ;
      mysql_free_result(res);
      return 0;
    }

    int num_fields = mysql_num_fields(res);
    MYSQL_FIELD *fields = mysql_fetch_fields(res);

    while ((row = mysql_fetch_row(res))) {
      string db_name;
      string table_name;
      size_t ordinal_position = 0;
      string column_name;
      string column_type;
      string charset;
      string column_key;

      for (int i=0; i<num_fields; i++) {

        if (strcmp(fields[i].name, "table_schema") == 0) {
          db_name = static_cast<string>(row[i]);
        } else if (strcmp(fields[i].name, "table_name") == 0) {
          table_name = static_cast<string>(row[i]);
        } else if (strcmp(fields[i].name, "ordinal_position") == 0) {
          ordinal_position = boost::lexical_cast<size_t>(row[i]);
        } else if (strcmp(fields[i].name, "column_name") == 0) {
          column_name = static_cast<string>(row[i]);
        } else if (strcmp(fields[i].name, "column_type") == 0) {
          column_type = static_cast<string>(row[i]);
        } else if (strcmp(fields[i].name, "character_set_name") == 0) {
          if (row[i] == NULL) {
            charset = "";
          } else {
            charset = static_cast<string>(row[i]);
          }
        } else if (strcmp(fields[i].name, "column_key") == 0) {
          column_key = static_cast<string>(row[i]);
        }
      }

      TableId key;
      key.db_name_ = db_name;
      key.table_name_ = table_name;
      if (table_map.find(key) == table_map.end()) {
        ColumnInfo one_column(column_name, column_type, charset,column_key);

        TableInfo one_table;
        one_table.db_name_ = db_name;
        one_table.table_name_ = table_name;
        one_table.schema_id_ = 0;  
        one_table.version_ = 0;

        one_table.column_map_[ordinal_position] = one_column;
        table_map[key] = one_table;

      } else {
        ColumnInfo one_column(column_name, column_type, charset, column_key);
        table_map[key].column_map_[ordinal_position] = one_column;
      }

    } // while

    mysql_free_result(res);

    return 0;
  }

  bool SchemaManager::IsDdlStatement(const string& query) {
    const char* ddl_sample[] = {
      "CREATE DATABASE",
      "CREATE SCHEMA",
      "CREATE INDEX",
      "CREATE TABLE",
      "DROP DATABASE",
      "DROP SCHEMA",
      "DROP INDEX",
      "DROP TABLE",
      "ALTER DATABASE",
      "ALTER TABLE",
      "RENAME TABLE"
    };

    for (unsigned int i=0; i<sizeof(ddl_sample)/sizeof(ddl_sample[0]); i++) {
      if (strncasecmp(query.c_str(), ddl_sample[i], strlen(ddl_sample[i])) == 0) {
        return true;
      }
    }
    return false;
  }

  string SchemaManager::QuoteEscapeString(const string& query) {
    char *buf = (char*) malloc(sizeof(char) * (query.size()*2+3));
    char *to = buf;
    *to++ = '\'';
    int escaped_length = mysql_real_escape_string(meta_db_conn_, to, query.c_str(), query.size());
    assert(escaped_length >= 0);
    to += escaped_length;
    *to++ = '\'';
    *to++ = '\0';
    string out(buf);
    free(buf);
    return out;
  }

  string SchemaManager::QuoteString(const string &query) {
    string out("\'");
    out.append(query);
    out.append("\'");
    return out;
  }

}
