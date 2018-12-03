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

#include "leader_election_test.h"
#include "rdp_environment.h"

namespace rdp_comm {

TEST_F(LeaderElectionTest, LeaderElection) {
  string zk_path = g_env->GetCfg()->GetValue<string>(string("zk.path"));
  LeaderElection *leader = new LeaderElection();
  ASSERT_TRUE(NULL != leader);
  string nodes_path = zk_path + g_leader_node_zk_path;
  string master_info_path = zk_path + g_leader_master_node_zk_path;
  bool rs;
  rs = leader->Initialize("127.0.0.1", nodes_path, master_info_path);
  ASSERT_TRUE(true == rs);

  LOG(INFO) << "Slee 10 Second";
  sleep(10);

  ASSERT_TRUE(true == leader->IsLeader());
  ASSERT_TRUE(false == leader->IsFollower());

  leader->Exit();
  LOG(INFO) << "Wait Leader Thread: " << leader->GetThreadId() << " Exit";
  int ret;
  if (0 != (ret = pthread_join(leader->GetThreadId(), NULL))) {
    LOG(INFO) << "Error: " << ret;
  }
  LOG(INFO) << "Leader Thread: " << leader->GetThreadId() << " Exit";
  delete leader;
  leader = NULL;
}
}  // namespace rdp_comm
