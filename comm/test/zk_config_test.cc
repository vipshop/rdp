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

#include "zk_config_test.h"

namespace rdp_comm {

void ZkConfigTest::SetUp() {
  string zk_path = g_env->GetCfg()->GetValue<string>("zk.path");
  zhandle_t *zk = g_env->GetZkHandle();
  string path = zk_path + "/ZkConfigTest";
  CHECKANDCREATEPATH(zk, zk_path);
  CHECKANDCREATEPATH(zk, path);
  CHECKANDCREATEPATH(zk, string(path + "/test1"));
  CHECKANDCREATEPATH(zk, string(path + "/test2"));
  CHECKANDCREATEPATH(zk, string(path + "/test3"));

  CHECKANDSETPATH(zk, string(path + "/test2" + "/test1"), "test2-test1-value");
  CHECKANDSETPATH(zk, string(path + "/test2" + "/test2"), "test2-test2-value");
  CHECKANDSETPATH(zk, string(path + "/test2" + "/test3"), "test2-test3-value");

  CHECKANDCREATEPATH(zk, string(path + "/test3" + "/test1"));
  CHECKANDCREATEPATH(zk, string(path + "/test3" + "/test2"));
  CHECKANDCREATEPATH(zk, string(path + "/test3" + "/test3"));
}

void ZkConfigTest::TearDown() {
  string zk_path = g_env->GetCfg()->GetValue<string>("zk.path");
  zhandle_t *zk = g_env->GetZkHandle();
  ZkRmR(zk, zk_path + "/ZkConfigTest");
}

TEST_F(ZkConfigTest, ZkConfig) {
  ZkConfig *zk_cfg = new ZkConfig();
  assert(NULL != zk_cfg);

  ZKProcess *zk_handle = &Singleton<ZKProcess>::GetInstance();
  assert(true == zk_handle->IsConnected());

  string zk_path = g_env->GetCfg()->GetValue<string>("zk.path");

  bool rs;
  vector<string> children;
  string value;

  rs = zk_cfg->Init(zk_handle, zk_path + "/xafasdfdsa");
  ASSERT_TRUE(false == rs);

  rs = zk_cfg->Init(zk_handle, zk_path + "/ZkConfigTest");
  ASSERT_TRUE(true == rs);

  rs = zk_cfg->CheckExists(zk_path + "/ZkConfigTest");
  ASSERT_TRUE(true == rs);
  rs = zk_cfg->GetChildrens(zk_path + "/ZkConfigTest", &children);
  ASSERT_TRUE(true == rs);
  ASSERT_EQ(3, children.size());
  rs = zk_cfg->GetValue(zk_path + "/ZkConfigTest", &value);
  ASSERT_TRUE(true == rs);
  ASSERT_TRUE(true == value.empty());
  rs = true;
  for (vector<string>::iterator it = children.begin(); it != children.end();
       ++it) {
    if (0 != it->compare("test1") && 0 != it->compare("test2") &&
        0 != it->compare("test3")) {
      rs = false;
      break;
    }
  }
  ASSERT_TRUE(true == rs);

  rs = zk_cfg->CheckExists(zk_path + "/ZkConfigTest" + "/test1");
  ASSERT_TRUE(true == rs);
  children.clear();
  rs = zk_cfg->GetChildrens(zk_path + "/ZkConfigTest" + "/test1", &children);
  ASSERT_TRUE(true == rs);
  ASSERT_TRUE(true == children.empty());
  rs = zk_cfg->GetValue(zk_path + "/ZkConfigTest", &value);
  ASSERT_TRUE(true == rs);
  ASSERT_TRUE(true == value.empty());
  value.clear();
  children.clear();

  rs = zk_cfg->CheckExists(zk_path + "/ZkConfigTest" + "/test2");
  ASSERT_TRUE(true == rs);
  rs = zk_cfg->GetChildrens(zk_path + "/ZkConfigTest" + "/test2", &children);
  ASSERT_TRUE(true == rs);
  ASSERT_EQ(3, children.size());
  rs = zk_cfg->GetValue(zk_path + "/ZkConfigTest" + "/test2", &value);
  ASSERT_TRUE(true == rs);
  ASSERT_TRUE(true == value.empty());
  rs = true;
  for (vector<string>::iterator it = children.begin(); it != children.end();
       ++it) {
    if (0 != it->compare("test1") && 0 != it->compare("test2") &&
        0 != it->compare("test3")) {
      rs = false;
      break;
    }
    bool flag = zk_cfg->GetValue(
        zk_path + "/ZkConfigTest" + "/test2" + "/" + *it, &value);
    ASSERT_TRUE(true == flag);
    if (0 == it->compare("test1")) {
      ASSERT_STREQ(value.c_str(), "test2-test1-value");
    }
    if (0 == it->compare("test2")) {
      ASSERT_STREQ(value.c_str(), "test2-test2-value");
    }
    if (0 == it->compare("test3")) {
      ASSERT_STREQ(value.c_str(), "test2-test3-value");
    }
  }
  ASSERT_TRUE(true == rs);
  value.clear();
  children.clear();

  rs = zk_cfg->CheckExists(zk_path + "/ZkConfigTest" + "/test3");
  ASSERT_TRUE(true == rs);
  rs = zk_cfg->GetChildrens(zk_path + "/ZkConfigTest" + "/test3", &children);
  ASSERT_TRUE(true == rs);
  ASSERT_EQ(3, children.size());
  rs = zk_cfg->GetValue(zk_path + "/ZkConfigTest" + "/test3", &value);
  ASSERT_TRUE(true == rs);
  ASSERT_TRUE(true == value.empty());
  rs = true;
  for (vector<string>::iterator it = children.begin(); it != children.end();
       ++it) {
    if (0 != it->compare("test1") && 0 != it->compare("test2") &&
        0 != it->compare("test3")) {
      rs = false;
      break;
    }
    bool flag = zk_cfg->GetValue(
        zk_path + "/ZkConfigTest" + "/test3" + "/" + *it, &value);
    ASSERT_TRUE(true == flag);
    ASSERT_TRUE(true == value.empty());
  }
  ASSERT_TRUE(true == rs);

  zhandle_t *zk = g_env->GetZkHandle();
  ASSERT_TRUE(NULL != zk);

  CHECKANDSETPATH(zk, string(zk_path + "/ZkConfigTest" + "/test3"),
                  "test3-test1-value");
  value.clear();
  rs = zk_cfg->GetValue(zk_path + "/ZkConfigTest" + "/test3", &value, true);
  ASSERT_TRUE(true == rs);

  rs = zk_cfg->Start();
  ASSERT_TRUE(true == rs);

  zk_cfg->Update(zk_path + "/ZkConfigTest" + "/test4", "test4-value1");
  zk_cfg->Update(zk_path + "/ZkConfigTest" + "/test4", "test4-value2");
  zk_cfg->Update(zk_path + "/ZkConfigTest" + "/test4", "test4-value3");
  zk_cfg->Update(zk_path + "/ZkConfigTest", "ZkConfigTest-value");

  while (0 != zk_cfg->GetUpdateSize()) {
    LOG(INFO) << "Wait Update Zookeeper: " << zk_cfg->GetUpdateSize();
    sleep(1);
  }
  rs = zk_cfg->Sync();
  ASSERT_TRUE(true == rs);

  value.clear();
  rs = zk_cfg->GetValue(zk_path + "/ZkConfigTest" + "/test4", &value);
  ASSERT_TRUE(true == rs);
  ASSERT_STREQ("test4-value3", value.c_str());

  value.clear();
  rs = zk_cfg->GetValue(zk_path + "/ZkConfigTest", &value);
  ASSERT_TRUE(true == rs);
  ASSERT_STREQ("ZkConfigTest-value", value.c_str());

  zk_cfg->Exit();

  delete zk_cfg;
}

}  // namespace rdp_comm
