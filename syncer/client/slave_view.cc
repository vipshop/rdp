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

#include "slave_view.h"
#include "syncer_app.h"

#include <rdp-comm/logger.h>
#include <rdp-comm/util.h>
#include <stdlib.h>
#include <boost/lexical_cast.hpp>
#include <boost/bind.hpp>

using namespace std;
using rdp_comm::MetricReporter;
using rdp_comm::CounterMetric;
using rdp_comm::SplitString;

namespace syncer {
  Slave::Slave(const string& host, int port, 
      const string& user, const string& password,
      uint64_t server_id, uint32_t interval_idle, uint32_t interval_busy) : 
    host_(host), port_(port), server_id_(server_id),
    user_(user), password_(password), 
    check_interval_idle_(interval_idle),
    check_interval_busy_(interval_busy),
    connected_(false), healthy_(false), need_evict_(false),
    conn_(NULL), sid_map_(NULL), gtid_executed_(&sid_map_,NULL),
    tid_(0) {

    gettimeofday(&last_check_time_, NULL);
  }

  Slave::~Slave() {
    Disconnect();
  }

  int Slave::Init() {
    if (Start() == false) {
      return -1;
    }
    return 0;
  }

  void Slave::Uninit() {
    exit_flag_ = true;
    if (tid_) {
      pthread_join(tid_, NULL);
    }
  }


  int Slave::Connect() {
    if (!conn_) {
      conn_ = mysql_init(NULL);
      if (!conn_) {
        RDP_LOG_ERROR << "mysql_init failed";
        return -1;
      }
      my_bool opt_value = true;

      if (mysql_options(conn_, MYSQL_OPT_RECONNECT, &opt_value)) {
        RDP_LOG_ERROR << "mysql_options failed";
        mysql_close(conn_);
        conn_ = NULL;
        return -1;
      }
    }

    if (!connected_) {
      RDP_LOG_INFO << "Connecting slave "<< host_ << ":" << port_ << "...";
      if (!mysql_real_connect(conn_, host_.c_str(), user_.c_str(), password_.c_str(),
          "", port_, NULL, 0)) {
        RDP_LOG_ERROR << "Connect mysql " << host_ << ":" << port_ << 
          " failed: " << mysql_error(conn_);
        mysql_close(conn_);
        conn_ = NULL;
        return -1;

      }
      if (mysql_set_character_set(conn_, "utf8")) {
        RDP_LOG_ERROR << "Set charset failed: " << mysql_error(conn_);
        mysql_close(conn_);
        conn_ = NULL;
        return -1;
      }
      connected_ = true;
    }
    return 0;

  }

  void Slave::Disconnect() {
    if (conn_) {
      RDP_LOG_INFO << "Closing slave connection "<< host_ << ":" << port_ << "...";
      mysql_close(conn_);
      conn_ = NULL;
      connected_ = false;
    }
  }

  // Check slave healthy, if behind master to long we think it is not healthy
  int Slave::CheckHealthy() {
    MYSQL_RES *res;
    MYSQL_ROW row;
    string io_running;
    string sql_running;
    char* seconds_behind_ptr = NULL;
    uint32_t seconds_behind = 0;

    string sql = " SHOW SLAVE STATUS ";
    //RDP_LOG_INFO << "Executing sql: " << sql ;
    if (mysql_real_query(conn_, sql.c_str(), sql.size())) {
      RDP_LOG_ERROR << "Execute sql failed: " << mysql_error(conn_);
      Disconnect();
      return -1;
    }

    res = mysql_store_result(conn_);
    if (!res) {
      RDP_LOG_ERROR << "Store mysql result failed: " << mysql_error(conn_);
      Disconnect();
      return -1;
    }
    if (mysql_num_rows(res) == 0) {
      RDP_LOG_WARN << "Slave status empty" ;
      mysql_free_result(res);
      return -1;
    }
    
    int num_fields = mysql_num_fields(res);
    MYSQL_FIELD *fields = mysql_fetch_fields(res);
    row = mysql_fetch_row(res);

    for (int i=0; i<num_fields; i++) {
      if (strcmp(fields[i].name, "Slave_IO_Running") == 0) {
        io_running = static_cast<string>(row[i]);
      } else if (strcmp(fields[i].name, "Slave_SQL_Running") == 0) {
        sql_running = static_cast<string>(row[i]);
      } else if (strcmp(fields[i].name, "Seconds_Behind_Master") == 0) {
        seconds_behind_ptr = row[i];
      }
    }
    
    if (io_running != "Yes"  || sql_running != "Yes") {
      RDP_LOG_WARN << "Slave " << host_ << ":" << port_ <<" io thread or sql thread not running" ;
      mysql_free_result(res);
      return -1;
    }

    if (seconds_behind_ptr != NULL) {
      seconds_behind = boost::lexical_cast<uint32_t>(seconds_behind_ptr);
    }
    (void)seconds_behind;

    mysql_free_result(res);

    return 0;
  }

  void Slave::AsyncWait(boost::shared_ptr<WaitGroup> wg) {
    mutex_guard guard(&lock_);

    if (IsGtidExecuted(wg->gtid_)) {
      // Slave has already executed it
      wg->Add(1);
      wg_.reset();
    } else {
      wg_ = wg;
      struct timeval cur_time;
      gettimeofday(&cur_time, NULL);
      uint64_t elapsed = (cur_time.tv_sec - cur_time.tv_sec) * 1000 + 
        (last_check_time_.tv_sec- last_check_time_.tv_usec) / 1000;
      if (elapsed >= check_interval_busy_) {
        // Need to wakeup the check thread
        Notify();
      }
    }
  }

  void Slave::Notify() {
      cond_.broadcast(); 
  }

  int Slave::GetGtidExecuted(string &gtid_set) {
    MYSQL_RES *res;    
    MYSQL_ROW row;
    string sql = "SELECT @@GLOBAL.gtid_executed";
    if (mysql_real_query(conn_, sql.c_str(), sql.size())) {
      RDP_LOG_ERROR << "Execute '" << sql << "' failed: " << mysql_error(conn_);
      Disconnect();
      return -1;
    }

    res = mysql_store_result(conn_);
    if (!res) {
      RDP_LOG_ERROR << "Store mysql result failed: " << mysql_error(conn_);
      Disconnect();
      return -1;
    }
    if (mysql_num_rows(res) != 1) {
      RDP_LOG_ERROR << "Result rows != 1" ;
      mysql_free_result(res);
      return -1;
    }

    row = mysql_fetch_row(res);
    gtid_set = boost::lexical_cast<string>(row[0]);
    mysql_free_result(res);

    return 0;
  }

  bool Slave::IsGtidExecuted(const string& gtid) {
    // Must already lock gtid_executed_
    Gtid_set tmp_set(&sid_map_, NULL);
    if (tmp_set.add_gtid_text(gtid.c_str()) != RETURN_STATUS_OK) {
      abort();
    }

    return tmp_set.is_subset(&gtid_executed_);
  }

  void Slave::Run() {
    char thread_name[64];
    string gtid_set;
    bool healthy;

    snprintf(thread_name, sizeof(thread_name),"slave%lu", server_id_);
    pthread_setname_np(pthread_self(), thread_name); 
    tid_ = pthread_self();

    exit_flag_ = false;
    while(!exit_flag_) {
      healthy = true;
      if (!connected_) {
        if (Connect()) {
          // Connect failed
          healthy = false;
        }
      }

      if (healthy && 
          (CheckHealthy() || GetGtidExecuted(gtid_set))) {
        healthy = false; 
      }

      {
        mutex_guard guard(&lock_);
        gettimeofday(&last_check_time_, NULL);

        if (!healthy && healthy_) {
          // healthy ---> not healthy
          RDP_LOG_WARN << "Slave "<< host_ << ":" << port_ << " become not healthy";
          char msg[255];
          snprintf(msg, sizeof(msg), "Slave %s:%d become not healthy", host_.c_str(), port_);
          g_syncer_app->Alarm(msg);
        }

        if (healthy && !healthy_) {
          RDP_LOG_WARN << "Slave "<< host_ << ":" << port_ << " become healthy";
        }


        healthy_ = healthy;
        if (healthy) {
          gtid_executed_.clear();
          if (gtid_executed_.add_gtid_text(gtid_set.c_str()) != RETURN_STATUS_OK) {
            abort();
          }
          if (wg_ && IsGtidExecuted(wg_->gtid_)) {
            // If someone waiting for a gtid, and it has been executed,
            // need to notify him 
            RDP_LOG_DBG << "Slave "<< host_ << ":" << port_ << " has executed " << wg_->gtid_;
            wg_->Add(1);
            wg_.reset();
          } else if (wg_) {
            // If someone waiting for a gtid, but has bot yet executed
            RDP_LOG_DBG << "Slave "<< host_ << ":" << port_ << " has not executed " << wg_->gtid_;
            cond_.wait(lock_.get(), check_interval_busy_);
            continue;
          } else {
            // No one waiting for a gtid
            RDP_LOG_INFO << "Slave "<< host_ << ":" << port_ << " gtid_executed is " << gtid_set;
          }
        } else {
          if (wg_) {
            RDP_LOG_WARN << "Slave "<< host_ << ":" << port_ << " is not healthy, skip " << wg_->gtid_;
            wg_->Add(1);
            wg_.reset();
          }
        }
        cond_.wait(lock_.get(), check_interval_idle_);
      }
    }

  }

  SlaveView::SlaveView() {
    inited_ = false;
    timeout_count_ = 0;
    slaves_count_metric_ = NULL;
    healthy_slaves_count_metric_ = NULL;
  }

  SlaveView::~SlaveView() {
    Uninit();
  }

  
  void SlaveView::Uninit() {
    MetricReporter* metric_reporter = g_syncer_app->metric_reporter_;
    if (slaves_count_metric_) {
      metric_reporter->UnRegistMetric(slaves_count_metric_);
      delete slaves_count_metric_;
      slaves_count_metric_ = NULL;
    }
    if (healthy_slaves_count_metric_) {
      metric_reporter->UnRegistMetric(healthy_slaves_count_metric_);
      delete healthy_slaves_count_metric_;
      healthy_slaves_count_metric_ = NULL;
    }
    inited_ = false;
  }

  int SlaveView::Init(const string& host, int port,
      const string& user, const string& password, 
      const string& included_server_id,
      uint32_t refresh_interval, uint32_t wait_timeout, 
      uint32_t check_interval_idle, uint32_t check_interval_busy) {
    MetricReporter* metric_reporter = g_syncer_app->metric_reporter_;
    master_host_ = host;
    master_port_ = port;
    user_ = user;
    password_ = password;
    refresh_interval_ = refresh_interval;
    wait_timeout_ = wait_timeout; 
    check_interval_idle_ = check_interval_idle;
    check_interval_busy_ = check_interval_busy;

    if (included_server_id.empty()) {
      RDP_LOG_ERROR << "server_id required.";
      return -1;
    }

    SplitString(included_server_id, ",", &included_server_id_);
    if (included_server_id_.size() == 0) {
      RDP_LOG_ERROR << "server_id required.";
      return -1;
    }

    if (Start() == false) {
      RDP_LOG_ERROR << "Start new thread failed. ";
      return -1;
    }

    slaves_count_metric_ = new CounterMetric("slaves_count", 10);
    slaves_count_metric_->Use(boost::bind(&SlaveView::GetSlavesCount, this));

    healthy_slaves_count_metric_ = new CounterMetric("healthy_slaves_count", 10);
    healthy_slaves_count_metric_->Use(boost::bind(&SlaveView::GetHealthySlavesCount, this));

    if (metric_reporter->RegistMetric(slaves_count_metric_)) {
      return -1;
    }
    if (metric_reporter->RegistMetric(healthy_slaves_count_metric_)) {
      return -1;
    }

    inited_ =  true;
    
    return 0;
  }

  bool SlaveView::IsIncluded(uint64_t server_id) {
    for (unsigned int i=0; i != included_server_id_.size(); i++) {
      char server_id_str[16];
      snprintf(server_id_str, sizeof(server_id_str), "%lu", server_id);
      if (included_server_id_[i] == server_id_str) {
        return true;
      }
    }
    return false; 
  }

  int SlaveView::GetSlavesFromMaster(vector<SlaveId>& keys) {
    MYSQL *conn = mysql_init(NULL);
    MYSQL_RES *res;
    MYSQL_ROW row;

    if (!conn) {
      RDP_LOG_ERROR << "mysql_init failed";
      return -1;
    }

    RDP_LOG_INFO << "Connecting master "<< master_host_ << ":" << master_port_ << "...";
    if (!mysql_real_connect(conn, master_host_.c_str(), user_.c_str(), password_.c_str(),
          "", master_port_, NULL, 0)) {
      RDP_LOG_ERROR << "Connect mysql " << master_host_ << ":" << master_port_ << 
        " failed: " << mysql_error(conn);
      mysql_close(conn);
      return -1;
    }

    if (mysql_set_character_set(conn, "utf8")) {
      RDP_LOG_ERROR << "Set charset failed: " << mysql_error(conn);
      mysql_close(conn);
      return -1;
    }


    string sql = " SHOW SLAVE HOSTS ";
    RDP_LOG_INFO << "Executing sql: " << sql ;
    if (mysql_real_query(conn, sql.c_str(), sql.size())) {
      RDP_LOG_ERROR << "Execute sql failed: " << mysql_error(conn);
      mysql_close(conn);
      return -1;
    }

    res = mysql_store_result(conn);
    if (!res) {
      RDP_LOG_ERROR << "Store mysql result failed: " << mysql_error(conn);
      mysql_close(conn);
      return -1;
    }
    if (mysql_num_rows(res) == 0) {
      RDP_LOG_WARN << "No slaves" ;
      mysql_free_result(res);
      mysql_close(conn);
      return 0;
    }
    
    int num_fields = mysql_num_fields(res);
    MYSQL_FIELD *fields = mysql_fetch_fields(res);
    while ((row = mysql_fetch_row(res))) {
      size_t server_id = 0;
      string host;
      int port = 0;

      for (int i=0; i<num_fields; i++) {
        if (strcmp(fields[i].name, "Server_id") == 0) {
          server_id = boost::lexical_cast<size_t>(row[i]);
        } else if (strcmp(fields[i].name, "Host") == 0) {
          host = static_cast<string>(row[i]);
        } else if (strcmp(fields[i].name, "Port") == 0) {
          port = boost::lexical_cast<int>(row[i]);
        }
      }
      if (!host.empty() && port && IsIncluded(server_id)) {
        SlaveId key;
        key.host_ = host;
        key.port_ = port;
        key.server_id_ = server_id;
        keys.push_back(key);
      }
    }
    mysql_free_result(res);
    mysql_close(conn);
  
    return 0;
  }

  void SlaveView::Run() {
    pthread_setname_np(pthread_self(), "slave_view"); 
    pthread_detach(pthread_self());

    exit_flag_ = false;
    while(!exit_flag_) {
      vector<SlaveId> keys;
      if (GetSlavesFromMaster(keys)) {
        usleep(refresh_interval_ * 1000);
        continue;
      }

      if (keys.size() == 0) {
        RDP_LOG_INFO << "No valid slaves";
      }
   
      { 
        mutex_guard guard(&lock_);

        // Mark all slaves to evict
        map<SlaveId, Slave*>::iterator it = slaves_.begin();
        for (;it != slaves_.end(); ++it) {
          it->second->MarkToEvict();
        }

        for (unsigned int i=0; i<keys.size(); i++) {
          SlaveId key = keys[i];
          if (slaves_.find(key) == slaves_.end()) {
            // New find slave
            RDP_LOG_INFO << "Find a new slave " << key.host_ << ":" << key.port_ << ", server_id: " << key.server_id_;
            Slave* s = new Slave(key.host_, key.port_, user_, password_, key.server_id_, 
                check_interval_idle_, check_interval_busy_);
            if (s->Init()) {
              delete s;
            } else {
              slaves_[key] = s;
            }
          } else {
            // The slave still work
            slaves_[key]->MarkToRemain();
          }
        }

        // Check which slave to evict
        for (it = slaves_.begin(); it != slaves_.end();) {
          if (it->second->NeedToEvict()) {
            // Evict it
            RDP_LOG_INFO << "Evict a slave " << it->second->host_ << ":" << it->second->port_;
            it->second->Uninit();
            delete it->second;
            slaves_.erase(it++);
            continue;
          }
          ++it;
        }
      }
   
      usleep(refresh_interval_ * 1000);
    }

  }

  int SlaveView::WaitGtid(const string& gtid) {
    if (gtid.empty()) {
      return 0;
    }
    // RDP_LOG_INFO << "Waiting " << gtid ;
    int count = 0;
    boost::shared_ptr<WaitGroup> wg(new WaitGroup);
    wg->gtid_ = gtid;

    {
      mutex_guard guard(&lock_);
      map<SlaveId, Slave*>::iterator it = slaves_.begin();
      for (;it != slaves_.end(); ++it) {
        SlaveId key = it->first;
        Slave *s = it->second;
        if (!s->IsHealthy()) {
          // RDP_LOG_INFO << "Slave " << key.host_ << ":" << key.port_ << " is not healthy";
          continue;
        }

        s->AsyncWait(wg);
        count++; 
      }
    }

    if (wg->Wait(count, wait_timeout_)) {
      RDP_LOG_WARN << "Wait " << gtid << " timeout";
      timeout_count_++;
      if ((timeout_count_*wait_timeout_/1000) % 300 ==0) {
        // Alarm every 5 minutes
        char msg[255];
        snprintf(msg, sizeof(msg), "Wait slave timeout %d times", timeout_count_);
        g_syncer_app->Alarm(msg);
      }
      return -1;
    }

    timeout_count_ = 0;

    return 0;
  }

}
