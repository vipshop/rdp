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

#ifndef RDP_COMM_LEADER_ELECTION_H
#define RDP_COMM_LEADER_ELECTION_H

#include "comm_incl.h"
#include "zk_process.h"
#include "threadbase.h"
#include "signal_handler.h"

#define LEASE_OF_ZK_RECV_TIME 0.5

using std::string;
using std::vector;

namespace rdp_comm {

class LeaderElection : public ThreadBase {
 public:
  enum WorkingMode {
     INIT_MODE = 0,
     FOLLOWER_MODE,
     LEADER_MODE,
     SAFE_MODE
  };

  explicit LeaderElection();
  ~LeaderElection();

  // leader_path store all nodes info zookeeper path
  // leader_info_path store master node info zookeeper path
  bool Initialize(const string &host, const string &leader_path, 
      const string &leader_info_path);

  inline void Exit(void) { exit_flag_ = true; }

  // Check I am leader
  inline bool IsLeader(void) { 
    bool ret;
    pthread_mutex_lock(&mode_lock_);
    ret = LEADER_MODE == working_mode_ || SAFE_MODE == working_mode_; 
    pthread_mutex_unlock(&mode_lock_);
    return ret;
  }

  // Check I am slave
  inline bool IsFollower(void) { return !IsLeader(); }

  WorkingMode GetMode(void) { 
    WorkingMode ret;
    pthread_mutex_lock(&mode_lock_);
    ret = working_mode_; 
    pthread_mutex_unlock(&mode_lock_);
    return ret;
  }

  void SetMode(WorkingMode mode) {
    pthread_mutex_lock(&mode_lock_);
    working_mode_ = mode;
    pthread_mutex_unlock(&mode_lock_);
    pthread_cond_signal(&mode_change_cond_);
  }

  // Return current znode's cversion
  uint64_t GetVersion(void);

  uint64_t GetEpoch(void) {
    return epoch_;
  }

  void SetEpoch(uint64_t epoch) {
    epoch_ = epoch;
  }

 private:

  // set node value store in zookeeper
  bool SetLeaderInfoValue(void) const;

  // check leader_path and leader_info_path
  // create if not exists
  bool CheckPath(void);

  // create EphAndSeq zookeeper node
  bool CreateElectionNode(void);

  // compare leader_info_path value and curr node name
  bool CheckLeaderNode(void);

  // Check zookeeper path
  bool CheckPathExists(const string &path);

  // Create zookeeper path
  bool CreatePath(const string &path);

  // If zookeeper expired session happend, reset state
  static void DefaultCallback(int type, int zk_state, const char *path,
                             void *data);

  // Watch leader_path path, change state
  static void ChildrenCallback(const string &path, const vector<string> &children,
                            void *ctx, const struct Stat *stat = NULL);

  virtual void Run(void);


  // Reset election node name
  inline void ResetNodeName(void) { node_name_.clear(); }

  // Get election node name
  inline string GetNodeName(void) const { return this->node_name_; }

  // Zookeeper handle
  ZKProcess *zk_;

  // The host name is used as value of the znode
  string host_;

  // EphAndSeq zookeeper node store path
  string leader_path_;

  // Zookeeper node store master info
  string leader_info_path_;

  // Election node name
  string node_name_;

  // Working mode
  WorkingMode working_mode_;
  pthread_mutex_t mode_lock_;
  pthread_cond_t mode_change_cond_;

  // election path zookeeper stat
  struct Stat election_path_stat_;
  
  bool exit_flag_;

  uint64_t epoch_;
};

}  // namespace rdp_comm

#endif  // RDP_COMM_LEADER_ELECTION_H
