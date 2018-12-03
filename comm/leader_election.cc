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

#include "leader_election.h"

namespace rdp_comm {

LeaderElection::LeaderElection() {
  zk_ = NULL;
  exit_flag_ = false;
  memset(&election_path_stat_, 0, sizeof(election_path_stat_));

  working_mode_ = INIT_MODE;
  pthread_cond_init(&mode_change_cond_, NULL);
  pthread_mutex_init(&mode_lock_, NULL);
}

LeaderElection::~LeaderElection() {}

bool LeaderElection::Initialize(const string &host, const string &leader_path,
                                const string &leader_info_path) {
  if (leader_path.empty()) {
    LOG(ERROR) << "Leader Path: " << leader_path << ", Is Empty";
    return false;
  }

  if (leader_info_path.empty()) {
    LOG(ERROR) << "Leader Store Info Path: " << leader_info_path
               << ", Is Empty";
    return false;
  }

  this->host_ = host;
  this->zk_ = &Singleton<ZKProcess>::GetInstance();
  this->leader_path_ = leader_path;
  this->leader_info_path_ = leader_info_path;
  this->working_mode_ = FOLLOWER_MODE;
  this->zk_->SetUserFuncAndCtx(&DefaultCallback, this);

  // Check the diretory of ephemeral node
  if (!CheckPath()) {
    return false;
  }

  // Start Election thread
  if (!Start()) {
    LOG(ERROR) << "Start LeaderElection thread start Failt";
    return false;
  }
  LOG(INFO) << "LeaderElection Thread Start";
  LOG(INFO) << "Zookeeper session timeout is " << zk_->GetRecvTimeout() << "ms";

  // Create ephemeral node
  if (!CreateElectionNode()) {
    node_name_.clear();
    return false;
  }

  // Create watch callback, and the watch will be triggered immediately
  if (!zk_->WatchChildren(leader_path_, &LeaderElection::ChildrenCallback, this, true)) {
    LOG(ERROR) << "Watch Path: " << leader_path_ << " Failt";
    return false;
  }

  return true;
}

bool LeaderElection::CheckPathExists(const string &path) {
  // save all nodes path stat, because EphAndSeqNode maybe created
  return zk_->Exists(path, &election_path_stat_);
}

bool LeaderElection::CreatePath(const string &path) {
  return zk_->CreateNode(path, "", true).Ok();
}

bool LeaderElection::CreateElectionNode() {
  if (!CheckPathExists(leader_path_)) {
    if (!CreatePath(leader_path_)) return false;

    // Sync path, confirm all zk nodes created
    if (!zk_->SyncPath(leader_path_)) {
      LOG(ERROR) << "Election Sync Zookeeper Path: " << leader_path_
                 << " Failt";
      return false;
    }

    // Check all election nodes store path and update the election path stat
    if (!CheckPathExists(leader_path_)) {
      LOG(ERROR) << "Leader Election All Nodes Store Path: " << leader_path_
                 << ", Create Failt";
      return false;
    }
  }

  string name = leader_path_;
  // value is the host name, node_name_ store return corrent node name
  if (!zk_->CreateEphAndSeqNode(name, host_, node_name_).Ok()) {
    LOG(ERROR) << "Create Leader Node: " << name << " Failt";
    return false;
  }

  // Sync path, confirm all zk nodes created
  if (!zk_->SyncPath(node_name_)) {
    LOG(ERROR) << "Election Sync Zookeeper Path: " << node_name_ << " Failt";
    return false;
  }

  return true;
}

void LeaderElection::Run() {
  char thd_name[16] = {0x0};
  snprintf(thd_name, sizeof(thd_name), "election.thd");
  rdp_comm::signalhandler::AddThread(pthread_self(), thd_name);

  WorkingMode old_mode = GetMode();

  while (!exit_flag_ && rdp_comm::signalhandler::IsRunning()) {
    switch (old_mode) {
      case FOLLOWER_MODE:
        LOG(INFO) << "I am follower!";
        break;

      case LEADER_MODE:
        LOG(INFO) << "I am leader!";

        if (!SetLeaderInfoValue()) {
          // If failed, still a leader
          LOG(ERROR) << "Set leader info: " << leader_info_path_ << " failt";
        }
        break;

      case SAFE_MODE:
        LOG(WARNING) << "I am leader in safe mode!";

        // TODO: Handle the EINTR
        // Sleep 0.4 * session timeout
        LOG(WARNING) << "Going to sleep " << zk_->GetRecvTimeout()*1000*0.4 << " us";
        usleep(zk_->GetRecvTimeout()*1000*0.4);

        if (SAFE_MODE == GetMode()) {
          LOG(ERROR) << "The zookeeper session is going to expired, exit now";
          exit(1);
        }

        break;

      default:
        LOG(ERROR) << "Unexpected working mode: " << old_mode << ", abort now";
        abort();
    }

    // Wait for mode changed, or timeout
    pthread_mutex_lock(&mode_lock_);
    while (old_mode == working_mode_) {
      int zk_timeout = zk_->GetRecvTimeout();
      struct timespec ts;
      clock_gettime(CLOCK_REALTIME, &ts);
      ts.tv_sec += zk_timeout/1000;
      ts.tv_nsec = (zk_timeout%1000)*1000*1000;
      if (pthread_cond_timedwait(&mode_change_cond_, &mode_lock_, &ts) == ETIMEDOUT) {
        break;
      }
    }
    old_mode = working_mode_;
    pthread_mutex_unlock(&mode_lock_);
  }

  // Quit from signal thread
  rdp_comm::signalhandler::Wait();

  return;
}

bool LeaderElection::CheckPath() {
  if (!CheckPathExists(leader_path_)) {
    if (!CreatePath(leader_path_)) {
      LOG(ERROR) << "Create Leader Path: " << leader_path_ << " Failt";
      return false;
    }
  }

  if (!CheckPathExists(leader_info_path_)) {
    if (!CreatePath(leader_info_path_)) {
      LOG(ERROR) << "Create Leader Store Info Path: " << leader_info_path_
                 << " Failt";
      return false;
    }
  }

  return true;
}

bool LeaderElection::SetLeaderInfoValue() const {
  return zk_->SetData(leader_info_path_, node_name_ + " " + StrTime(NULL));
}

void LeaderElection::DefaultCallback(int type, int zk_state, const char *path,
                                     void *data) {
  LeaderElection *leader = static_cast<LeaderElection *>(data);
  if (ZOO_SESSION_EVENT == type) {
    if (ZOO_CONNECTING_STATE == zk_state) {
      // Have no effect on FOLLOWER_MODE
      if (leader->GetMode() == LEADER_MODE) {
        LOG(WARNING) << "I enter safe mode!";
        leader->SetMode(SAFE_MODE);
      }

    } else if (ZOO_CONNECTED_STATE == zk_state) {
      // Have no effect on FOLLOWER_MODE
      if (leader->GetMode() == SAFE_MODE) {
        LOG(WARNING) << "I leave safe mode!";
        leader->SetMode(LEADER_MODE);
      }

    } else if (ZOO_EXPIRED_SESSION_STATE == zk_state) {
      LOG(ERROR) << "Zookeeper ession expired, exit now";
      exit(1);
    } else {
      LOG(ERROR) << "Unkown zookeeper session event, abort now";
      abort();
    }
  }
}

void LeaderElection::ChildrenCallback(const string &path,
                                      const vector<string> &children, void *ctx,
                                      const struct Stat *stat) {
  // no node create
  if (0 >= children.size()) return;

  assert(NULL != ctx);

  LeaderElection *leader = static_cast<LeaderElection *>(ctx);

  // The ephemeral node doesn't exist
  if (leader->GetNodeName().empty()) {
    LOG(ERROR) << "The ephemeral node doesn't exist, abort now";
    abort();
  }

  long min_child = atol(children[0].c_str());
  for (unsigned int i = 0; i < children.size(); ++i) {
    min_child = min_child > atol(children[i].c_str())
                    ? atol(children[i].c_str())
                    : min_child;
  }
  string my_name = leader->GetNodeName();

  // get my node name`s seq no
  long my_index = atol(my_name.substr(my_name.length() - children[0].length(),
                                      children[0].length()).c_str());

  LOG(INFO) << "My Node Name: " << my_index
            << ", Minimum Sequence Node Name: " << min_child;

  //如果本节点创建的node的index为最小，则可以提升为leader
  if (my_index == min_child) {
    if (leader->IsFollower()) {
      LOG(INFO) << "I become leader!";
      leader->SetMode(LEADER_MODE);
    }
  } else {
    if (leader->IsLeader()) {
      // Become follwer unexpectly
      LOG(ERROR) << "Become follower unexpectly, abort now";
      abort();
    }
  }
}

// Return current znode's cversion
uint64_t LeaderElection::GetVersion() {
  return (uint64_t)election_path_stat_.cversion;
}

}  // namespace rdp_comm
