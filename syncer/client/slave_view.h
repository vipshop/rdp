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

#ifndef __SLAVE_VIEW_H__
#define __SLAVE_VIEW_H__

#include <rdp-comm/threadbase.h>
#include <rdp-comm/metric_reporter.h>
#include "syncer_main.h"
#include "syncer_incl.h"

#include <map>
#include <vector>
#include <string>
#include <boost/shared_ptr.hpp>

namespace syncer {

class WaitGroup {
  public:
    WaitGroup() :
      count_(0), target_count_(-1) {
    }
    ~WaitGroup() {
    }
    void Add(int value) { 
      mutex_guard guard(&mutex_);
      count_+=value; 
      if (count_ >= target_count_) {
        cond_.broadcast();
      }
    }

    // Wait counter up to specified count
    int Wait(int count, int timeout) { 
      mutex_guard guard(&mutex_);
      target_count_ = count;
      if (count_ >= target_count_) {
        // Already enough, not need to wait
        return 0;
      }
      return cond_.wait(mutex_.get(), timeout);
    }

    std::string gtid_;

  private:
    int count_;
    int target_count_;
    mutex_t mutex_;
    cond_t cond_;
};

class Slave : public rdp_comm::ThreadBase {
 public:
  Slave(const std::string& host, int port, 
      const std::string& user, const std::string& password,
      uint64_t server_id, uint32_t interval_idle, uint32_t interval_busy);
  ~Slave();


  virtual void Run();

  int Init(); 
  void Uninit(); 
  bool IsHealthy() { 
    mutex_guard guard(&lock_);
    return healthy_; 
  }

  // If this slave is not in slave list of master, need evict it
  void MarkToEvict() { need_evict_ = true; }
  void MarkToRemain() { need_evict_ = false; }
  bool NeedToEvict() { return need_evict_; }

  void AsyncWait(boost::shared_ptr<WaitGroup> wg);

  std::string host_;
  int port_;
  uint64_t server_id_;
  
 private:
  int Connect();
  void Disconnect();

  // Check slave's status
  int CheckHealthy();

  // Get slave's gtid executed set
  int GetGtidExecuted(std::string &gtid);
  bool IsGtidExecuted(const std::string &gtid);

  // Notify check-thread to wakeup
  void Notify();

  std::string user_;
  std::string password_;

  // Interval to query slave gtid_executed
  uint32_t check_interval_idle_;
  uint32_t check_interval_busy_;


  bool connected_;
  bool healthy_; // Need to be protected by lock
  bool need_evict_;

  MYSQL *conn_;

  mutex_t lock_;
  cond_t cond_;

  boost::shared_ptr<WaitGroup> wg_; // Need to be protected by lock
  volatile bool exit_flag_;

  // For gtid set operations
  Sid_map sid_map_;  
  Gtid_set gtid_executed_; // Need to be protected by lock

  timeval last_check_time_; // Need to be protected by lock 

  pthread_t tid_;
};


class SlaveId {
 public:
   SlaveId() : port_(0), server_id_(0) {}
   bool operator<(const SlaveId& rhs) const {
     int rc = strcmp(host_.c_str(), rhs.host_.c_str());
     if (rc < 0) {
       return true;
     } else if (rc == 0) {
       return port_ < rhs.port_;
     } else {
       return false;
     }
   }
   bool operator==(const SlaveId& rhs) const {
     return host_ == rhs.host_ &&
       port_ == rhs.port_;
   }
   std::string host_;
   int port_;
   uint64_t server_id_;
};



class SlaveView : public rdp_comm::ThreadBase {
 public:
  SlaveView();
  ~SlaveView();

  int Init(const std::string& host, int port,
      const std::string& user, const std::string& password,
      const std::string& included_server_id,
      uint32_t refresh_interval, uint32_t wait_timeout,
      uint32_t check_interval_idle, uint32_t check_interval_busy);
  void Uninit();
  bool IsInited() { return inited_; }
  int WaitGtid(const std::string &gtid);

  virtual void Run();

  long GetSlavesCount() {
    mutex_guard guard(&lock_);
    return slaves_.size();
  }

  long GetHealthySlavesCount() {
    int count = 0;
    mutex_guard guard(&lock_);
    std::map<SlaveId, Slave*>::iterator it = slaves_.begin();
    for (;it != slaves_.end(); ++it) {
      Slave *s = it->second;
      if (!s->IsHealthy()) {
        continue;
      }
      count++; 
    }
    return count;
  }

 private:
  int GetSlavesFromMaster(std::vector<syncer::SlaveId>&);
  
  bool IsIncluded(uint64_t server_id);

  std::string master_host_;
  int master_port_;
  std::string user_;
  std::string password_;

  // Max time to wait all healhty slave catch up the specified gtid
  uint32_t wait_timeout_;

  // Interval to refresh slave list
  uint32_t refresh_interval_;

  // Interval to query gtid and check healty
  uint32_t check_interval_idle_;
  uint32_t check_interval_busy_;

  std::map<SlaveId, Slave*> slaves_;
  mutex_t lock_;

  bool inited_;
  volatile bool exit_flag_;
  // Wait timeout times
  int timeout_count_;

  std::vector<std::string> included_server_id_;

  rdp_comm::CounterMetric *slaves_count_metric_;
  rdp_comm::CounterMetric *healthy_slaves_count_metric_;

};

}


#endif
