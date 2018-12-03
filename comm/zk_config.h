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

#ifndef RDP_ZK_CONFIG_H
#define RDP_ZK_CONFIG_H

#include "comm_incl.h"
#include "zk_process.h"
#include "threadbase.h"
#include "signal_handler.h"

using std::string;
using std::map;
using std::vector;
using std::queue;

namespace rdp_comm {

class ZkConfig : public ThreadBase {
 public:
  typedef struct Node {
    // children nodes name
    vector<string> sub_node;
    // node string value
    string value;
  } Node;

  typedef map<string, Node *> NodeMap;
  // wait for update zookeeper nodes
  typedef struct UpdateNode {
    string key;
    string value;
  } UpdateNode;

  typedef queue<UpdateNode *> UpdateQueue;
  typedef vector<UpdateNode *> UpdateVector;

 public:
  // \brief ZkConfig
  explicit ZkConfig();

  // \brief ~ZkConfig
  ~ZkConfig();

  // \brief Init
  //
  // \param zk: zookeeper process handle
  // \param zk_root_path: zookeeper root node
  //
  // \return true
  bool Init(ZKProcess *zk, const string &zk_root_path);

  void Update(const string &key, const string &value);

  bool CheckExists(const string &key, bool sync = false);

  bool GetChildrens(const string &key, vector<string> *childrens,
                    bool sync = false);

  bool GetValue(const string &key, string *value, bool sync = false);

  bool Sync(void);

  void Clear(void);

  inline void Exit(void) { exit_flag_ = true; }

  inline int GetUpdateSize(void) { return update_nodes_.size(); }

 protected:
  void Run(void);

 private:
  void ClearNodes(void);
  void ClearUpdateQueue(void);

 private:
  string zk_root_path_;
  ZKProcess *zk_handle_;

  spinlock_t nodes_lock_;

  NodeMap nodes_;
  UpdateQueue update_nodes_;

  pthread_cond_t empty_cond_;
  pthread_mutex_t mutex_;

  volatile bool exit_flag_;
};

}  // namespace rdp_comm

#endif  // RDP_ZK_CONFIG_H
