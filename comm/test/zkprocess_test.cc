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

#include "zkprocess_test.h"
#include "rdp_environment.h"

static void ThisChildrenWatcherCallback(const string &path,
                                        const vector<string> &value, void *ctx,
                                        const struct Stat *stat) {
  vector<string> *rs = static_cast<vector<string> *>(ctx);
  for (vector<string>::const_iterator it = value.begin(); it != value.end();
       ++it) {
    rs->push_back(*it);
  }
}

static void WatchDataCallback(const string &path, const string &value,
                              void *ctx, const struct Stat *stat) {
  string *rs = static_cast<string *>(ctx);
  rs->assign(value);
}

namespace rdp_comm {

TEST_F(ZKProcessTest, ZKProcess) {

  ZKProcess *zk_process = &Singleton<ZKProcess>::GetInstance();
  assert(NULL != zk_process);
  ASSERT_TRUE(true == zk_process->IsConnected());
  string zk_path = g_env->GetCfg()->GetValue<string>("zk.path");
  bool rs;
  zhandle_t *zk = g_env->GetZkHandle();
  int ret = 0;
  {
    // CreateNode
    string node = zk_path + "/CreateNode1";
    string value;
    ret = 0;
    zhandle_t *zk = g_env->GetZkHandle();
    ret = zoo_exists(zk, node.c_str(), 0, NULL);
    if (ZOK == ret) {
      rdp_comm::ZkRmR(zk, node);
      ret = zoo_exists(zk, node.c_str(), 0, NULL);
    }
    ASSERT_FALSE(ZOK == ret);
    rs = zk_process->SyncPath(node);
    ASSERT_TRUE(true == rs);
    ASSERT_TRUE(true == zk_process->CreateNode(node, value, false).Ok());
    ret = zoo_exists(zk, node.c_str(), 0, NULL);
    EXPECT_TRUE(ZOK == ret) << " MSG:" << rdp_comm::ErrorStr(ret);

    string node2 = zk_path + "/CreateNode2/Recursive";
    ret = zoo_exists(zk, node2.c_str(), 0, NULL);
    if (ZOK == ret) {
      rdp_comm::ZkRmR(zk, node2);
      ret = zoo_exists(zk, node2.c_str(), 0, NULL);
    }
    ASSERT_FALSE(ZOK == ret);
    rs = zk_process->SyncPath(node2);
    ASSERT_TRUE(true == rs);
    ASSERT_TRUE(true == zk_process->CreateNode(node2, value, true).Ok());
    rs = zk_process->SyncPath(node2);
    ASSERT_TRUE(true == rs);
    ret = zoo_exists(zk, node2.c_str(), 0, NULL);
    EXPECT_TRUE(ZOK == ret) << " MSG:" << rdp_comm::ErrorStr(ret);
  }

  {
    // GetData
    string node = zk_path + "/GetData";
    string value = "GetData";
    CHECKANDSETPATH(zk, node, value.c_str());
    rs = zk_process->SyncPath(node);
    ASSERT_TRUE(true == rs);
    string rvalue;
    rs = zk_process->GetData(node, rvalue);
    ASSERT_TRUE(true == rs);
    EXPECT_STREQ(value.c_str(), rvalue.c_str());
  }

  {
    // SetData
    string node = zk_path + "/SetData";
    string value = "GetData";
    CHECKANDCREATEPATH(zk, node);
    rs = zk_process->SyncPath(node);
    ASSERT_TRUE(true == rs);
    rs = zk_process->SetData(node, value);
    ASSERT_TRUE(true == rs);
    char buffer[32] = {0};
    int buffer_size = sizeof(buffer);
    ret = zoo_get(zk, node.c_str(), 0, buffer, &buffer_size, NULL);
    ASSERT_TRUE(ZOK == ret) << " MSG:" << rdp_comm::ErrorStr(ret);
    EXPECT_STREQ(value.c_str(), buffer);
  }

  {
    // Exists
    string node = zk_path + "/Exists";
    rs = zk_process->Exists(node);
    ret = zoo_exists(zk, node.c_str(), 0, NULL);
    if (rs) {
      EXPECT_TRUE(ZOK == ret) << " MSG:" << rdp_comm::ErrorStr(ret);
    } else {
      EXPECT_FALSE(ZOK == ret) << " MSG:" << rdp_comm::ErrorStr(ret);
    }
  }

  {
    // GetChildren
    string node = zk_path + "/GetChildren";
    CHECKANDCREATEPATH(zk, node);
    rs = zk_process->SyncPath(node);
    ASSERT_TRUE(true == rs);

    for (int i = 0; i < 5; ++i) {
      string path = node + "/" + to_string(i);
      ret = zoo_create(zk, path.c_str(), NULL, 0, &ZOO_OPEN_ACL_UNSAFE, 0, NULL,
                       0);
      ASSERT_TRUE(ZOK == ret) << " MSG:" << rdp_comm::ErrorStr(ret);
    }
    rs = zk_process->SyncPath(node);
    ASSERT_TRUE(true == rs);

    String_vector sv;
    ret = zoo_get_children(zk, node.c_str(), 0, &sv);
    ASSERT_TRUE(ZOK == ret) << " MSG:" << rdp_comm::ErrorStr(ret);
    vector<string> children;
    int rs = zk_process->GetChildren(node, children);
    ASSERT_TRUE(true == rs);
    bool flag = false;
    for (int i = 0; i < sv.count; ++i) {
      flag = false;
      for (vector<string>::iterator it = children.begin(); it != children.end();
           ++it) {
        if (0 == it->compare(sv.data[i])) {
          flag = true;
          break;
        }
      }
      if (flag == false) break;
    }
    EXPECT_TRUE(true == flag);
    deallocate_String_vector(&sv);
  }

  {
    // WatchData
    string node = zk_path + "/WatchData";
    string data;
    CHECKANDCREATEPATH(zk, node);
    rs = zk_process->SyncPath(node);
    ASSERT_TRUE(true == rs);
    rs = zk_process->WatchData(node, &WatchDataCallback,
                               static_cast<void *>(&data));
    ASSERT_TRUE(true == rs);
    string value = "WatchData";
    ret = zoo_set(zk, node.c_str(), value.c_str(), value.length(), -1);
    ASSERT_TRUE(ZOK == ret) << " MSG:" << rdp_comm::ErrorStr(ret);
    LOG(INFO) << "Sleep 1 second Wait For CallBack";
    rs = zk_process->SyncPath(node);
    ASSERT_TRUE(true == rs);
    EXPECT_STREQ(value.c_str(), data.c_str());
  }

  {
    // WatchChildren
    string node = zk_path + "/WatchChildren";
    vector<string> children;
    CHECKANDCREATEPATH(zk, node);
    rs = zk_process->SyncPath(node);
    ASSERT_TRUE(true == rs);
    rs = zk_process->WatchChildren(node, ThisChildrenWatcherCallback,
                                   static_cast<vector<string> *>(&children));
    ASSERT_TRUE(true == rs);
    vector<string> children_add;
    for (int i = 0; i < 5; ++i) {
      string path = node + "/" + to_string(i);
      ret = zoo_create(zk, path.c_str(), NULL, 0, &ZOO_OPEN_ACL_UNSAFE, 0, NULL,
                       0);
      ASSERT_TRUE(ZOK == ret) << " MSG:" << rdp_comm::ErrorStr(ret);
      children_add.push_back(to_string(i));
    }
    LOG(INFO) << "Sleep 1 second Wait For CallBack";
    rs = zk_process->SyncPath(node);
    ASSERT_TRUE(true == rs);
    bool flag = false;
    for (vector<string>::iterator a = children_add.begin();
         a != children_add.end(); ++a) {
      flag = false;
      for (vector<string>::iterator b = children.begin(); b != children.end();
           ++b) {
        if (0 == a->compare(*b)) {
          flag = true;
          break;
        }
      }
      if (flag == false) {
        break;
      }
    }
    EXPECT_TRUE(true == flag);
  }

  {
    // GetHosts
    string zk_hosts = g_env->GetCfg()->GetValue<string>("zk.hosts");
    EXPECT_STREQ(zk_hosts.c_str(), zk_process->GetHosts().c_str());
  }

  {
    // GetRecvTimeout
    zhandle_t *zk = g_env->GetZkHandle();
    EXPECT_EQ(zk_process->GetRecvTimeout(), zoo_recv_timeout(zk));
  }

  {
    // CreateEphAndSeqNode
    string node = zk_path + "/EphAndSeqNode";
    string value;
    string rvalue;

    ASSERT_TRUE(true == zk_process->CreateEphAndSeqNode(node, value, rvalue));
    rs = zk_process->SyncPath(node);
    ASSERT_TRUE(true == rs);
    EXPECT_TRUE(ZOK == zoo_exists(zk, rvalue.c_str(), 0, NULL));
  }
}

}  // namespace rdp_comm
