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

#include "zk_config.h"

namespace rdp_comm {

static bool DeepGetNode(ZKProcess *zk_handle, ZkConfig::NodeMap &nodes,
                        const string &path) {
  if (!zk_handle->Exists(path)) {
    LOG(ERROR) << "Zookeeper Path: " << path << " Not Exists";
    return false;
  }

  ZkConfig::Node *node = new ZkConfig::Node;
  assert(NULL != node);
  // get path value
  if (!zk_handle->GetData(path, node->value)) {
    LOG(ERROR) << "Zookeeper Get Path: " << path << " Value Failt";
    delete node;
    return false;
  }
  // get path children
  if (!zk_handle->GetChildren(path, node->sub_node)) {
    LOG(ERROR) << "Zookeeper Get Path: " << path << " Children Failt";
    delete node;
    return false;
  }
  // save node
  nodes[path] = node;
  // recursive get children path
  for (vector<string>::iterator it = node->sub_node.begin();
       it != node->sub_node.end(); ++it) {
    if (!DeepGetNode(zk_handle, nodes, string(path + "/" + (*it)))) {
      LOG(ERROR) << "DeepGetNode Path: " << string(path + "/" + (*it))
                 << " Failt";
      return false;
    }
  }
  return true;
}

static void ThreadCleanupHandler(void *args) {
  ZkConfig *zk_cfg = static_cast<ZkConfig *>(args);
  if (NULL != zk_cfg) {
    zk_cfg->Clear();
  }
}

ZkConfig::ZkConfig() : zk_handle_(NULL), exit_flag_(false) {
  pthread_mutex_init(&mutex_, NULL);
  pthread_cond_init(&empty_cond_, NULL);
}

ZkConfig::~ZkConfig() {
  ClearNodes();
  ClearUpdateQueue();

  pthread_cond_destroy(&empty_cond_);
  pthread_mutex_destroy(&mutex_);
}

bool ZkConfig::Init(ZKProcess *zk, const string &zk_root_path) {
  assert(NULL != zk);
  if (!zk->IsConnected()) {
    LOG(ERROR) << "ZKProcess Is Not Connected To Zookeeper";
    return false;
  }
  zk_handle_ = zk;

  if (!zk_handle_->Exists(zk_root_path)) {
    LOG(WARNING) << "Zookeeper Path: " << zk_root_path << " Not Exists";
    return false;
  }
  zk_root_path_ = zk_root_path;

  lock_guard guard(&nodes_lock_);

  if (!DeepGetNode(zk_handle_, nodes_, zk_root_path_)) {
    LOG(ERROR) << "Get Zookeeper Dir Struct And Data Of Path: " << zk_root_path_
               << " Failt";
    ClearUpdateQueue();
    return false;
  }
  return true;
}

void ZkConfig::ClearNodes() {
  lock_guard guard(&nodes_lock_);

  for (NodeMap::iterator it = nodes_.begin(); it != nodes_.end(); ++it) {
    delete it->second;
  }
  nodes_.clear();
}

void ZkConfig::ClearUpdateQueue() {
  UpdateNode *update_node = NULL;
  while (!update_nodes_.empty()) {
    update_node = update_nodes_.front();
    if (NULL != update_node) {
      delete update_node;
    }
    update_nodes_.pop();
  }
}

bool ZkConfig::Sync() {
  ClearNodes();

  if (!DeepGetNode(zk_handle_, nodes_, zk_root_path_)) {
    LOG(ERROR) << "Get Zookeeper Dir Struct And Data Of Path: " << zk_root_path_
               << " Failt";
    ClearNodes();
    return false;
  }
  return true;
}

void ZkConfig::Update(const string &key, const string &value) {
  pthread_mutex_lock(&mutex_);
  UpdateNode *update_node = new UpdateNode;
  assert(NULL != update_node);
  update_node->key = key;
  update_node->value = value;
  update_nodes_.push(update_node);
  pthread_cond_signal(&empty_cond_);
  pthread_mutex_unlock(&mutex_);
}

bool ZkConfig::CheckExists(const string &key, bool sync) {
  if (sync && !Sync()) {
    LOG(ERROR) << "Synchronize Zookeeper Data Failt";
    return false;
  }
  NodeMap::iterator found = nodes_.find(key);
  if (found == nodes_.end()) {
    return false;
  } else {
    return true;
  }
}

bool ZkConfig::GetChildrens(const string &key, vector<string> *childrens,
                            bool sync) {
  if (sync && !Sync()) {
    LOG(ERROR) << "Synchronize Zookeeper Data Failt";
    return false;
  }

  assert(NULL != childrens);
  NodeMap::iterator found = nodes_.find(key);
  if (found != nodes_.end()) {
    Node *node = found->second;
    assert(NULL != node);
    for (vector<string>::iterator it = node->sub_node.begin();
         it != node->sub_node.end(); ++it) {
      childrens->push_back(*it);
    }
    return true;
  } else {
    childrens->clear();
    return false;
  }
}

bool ZkConfig::GetValue(const string &key, string *value, bool sync) {
  if (sync && !Sync()) {
    LOG(ERROR) << "Synchronize Zookeeper Data Failt";
    return false;
  }

  assert(NULL != value);
  NodeMap::iterator found = nodes_.find(key);
  if (found != nodes_.end()) {
    Node *node = found->second;
    assert(NULL != node);
    value->assign(node->value);
    return true;
  } else {
    return false;
  }
}

void ZkConfig::Run() {
  LOG(INFO) << "Start Zookeeper Configure Thread";

  pthread_detach(pthread_self());
  pthread_cleanup_push(ThreadCleanupHandler, this);

  int update_failt_time = 0;
  char thd_name[16] = {0x0};
  UpdateVector update_nodes_tmp;
  snprintf(thd_name, sizeof(thd_name), "zkconfig.thd");
  rdp_comm::signalhandler::AddThread(pthread_self(), thd_name);
  while (!exit_flag_ && rdp_comm::signalhandler::IsRunning()) {
    pthread_mutex_lock(&mutex_);
    while (!exit_flag_ && update_nodes_.empty()) {
      pthread_cond_wait(&empty_cond_, &mutex_);
    }

    UpdateNode *node = NULL;
    for (int i = 0; i < 20 && !update_nodes_.empty(); ++i) {
      node = update_nodes_.front();
      if (NULL != node) {
        update_nodes_tmp.push_back(node);
      }
      update_nodes_.pop();
    }
    pthread_mutex_unlock(&mutex_);

    for (UpdateVector::iterator it = update_nodes_tmp.begin();
         it != update_nodes_tmp.end();) {
      node = *it;

      if (!zk_handle_->SetData(node->key, node->value)) {
        LOG(ERROR) << "Update Zookeeper Path: " << node->key
                   << ", Value: " << node->value << " Failt";

        ++update_failt_time;

        // wait reconnect to zk
        usleep(100);
        if (update_failt_time > 20) {
          LOG(ERROR) << "Retry: " << update_failt_time
                     << " Times Update Zookeeper, Still Failt";
          exit_flag_ = true;
        }
      } else {
        ++it;
        update_failt_time = 0;

        DLOG(INFO) << "Update Zookeeper Path: " << node->key
                   << ", Value: " << node->value << " Success";

        delete node;
        node = NULL;
      }
    }

    // if exit_flag_ true, update_nodes_tmp need delete node
    if (!exit_flag_) {
      update_nodes_tmp.clear();
    }
  }

  if (!update_nodes_tmp.empty()) {
    for (UpdateVector::iterator it = update_nodes_tmp.begin();
         it != update_nodes_tmp.end(); ++it) {
      if (NULL != (*it)) {
        delete (*it);
      }
    }
    update_nodes_tmp.clear();
  }

  // Quit parsing from signal thread
  rdp_comm::signalhandler::Wait();
  pthread_cleanup_pop(1);

  LOG(INFO) << "Zookeeper Configure Thread Stop";
}

void ZkConfig::Clear() {
  pthread_mutex_lock(&mutex_);
  UpdateNode *node = NULL;
  while (NULL != (node = update_nodes_.front())) {
    update_nodes_.pop();
    // ignore update success or failt
    zk_handle_->SetData(node->key, node->value);
    delete node;
    node = NULL;
  }
  pthread_mutex_unlock(&mutex_);
}

}  // namespace rdp_comm
