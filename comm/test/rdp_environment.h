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

#ifndef RDP_TEST_ENVIRONMENT_H
#define RDP_TEST_ENVIRONMENT_H

#include <gtest/gtest.h>
#include "../comm_incl.h"
#include "../config.h"
#include "../zk_process.h"

#define CHECKANDCREATEPATH(handle, path)                                       \
  do {                                                                         \
    int ret = zoo_exists(handle, path.c_str(), 0, NULL);                       \
    if (ZOK != ret) {                                                          \
      ret = zoo_create(handle, path.c_str(), NULL, 0, &ZOO_OPEN_ACL_UNSAFE, 0, \
                       NULL, 0);                                               \
      ASSERT_TRUE(ZOK == ret) << " MSG:" << rdp_comm::ErrorStr(ret);           \
    }                                                                          \
  } while (0);

#define CHECKANDSETPATH(handle, path, value)                         \
  do {                                                               \
    int ret = zoo_exists(handle, path.c_str(), 0, NULL);             \
    if (ZOK != ret) {                                                \
      ret = zoo_create(handle, path.c_str(), value, strlen(value),   \
                       &ZOO_OPEN_ACL_UNSAFE, 0, NULL, 0);            \
      ASSERT_TRUE(ZOK == ret) << " MSG:" << rdp_comm::ErrorStr(ret); \
    } else {                                                         \
      ret = zoo_set(handle, path.c_str(), value, strlen(value), -1); \
      ASSERT_TRUE(ZOK == ret) << " MSG:" << rdp_comm::ErrorStr(ret); \
    }                                                                \
  } while (0);

#define CHECKANDCREATEPATH1(handle, path)                                      \
  do {                                                                         \
    int ret = zoo_exists(handle, path.c_str(), 0, NULL);                       \
    if (ZOK != ret) {                                                          \
      ret = zoo_create(handle, path.c_str(), NULL, 0, &ZOO_OPEN_ACL_UNSAFE, 0, \
                       NULL, 0);                                               \
      if (ZOK != ret) {                                                        \
        LOG(ERROR) << " MSG:" << rdp_comm::ErrorStr(ret);                      \
      }                                                                        \
      return;                                                                  \
    }                                                                          \
  } while (0);

#define CHECKANDSETPATH1(handle, path, value)                        \
  do {                                                               \
    int ret = zoo_exists(handle, path.c_str(), 0, NULL);             \
    if (ZOK != ret) {                                                \
      ret = zoo_create(handle, path.c_str(), value, strlen(value),   \
                       &ZOO_OPEN_ACL_UNSAFE, 0, NULL, 0);            \
      if (ZOK != ret) {                                              \
        LOG(ERROR) << " MSG:" << rdp_comm::ErrorStr(ret);            \
      }                                                              \
      return;                                                        \
    } else {                                                         \
      ret = zoo_set(handle, path.c_str(), value, strlen(value), -1); \
      if (ZOK != ret) {                                              \
        LOG(ERROR) << " MSG:" << rdp_comm::ErrorStr(ret);            \
      }                                                              \
      return;                                                        \
    }                                                                \
  } while (0);

#define CHECKANDCREATEPATH2(handle, path, rs)                                  \
  do {                                                                         \
    int ret = zoo_exists(handle, path.c_str(), 0, NULL);                       \
    if (ZOK != ret) {                                                          \
      ret = zoo_create(handle, path.c_str(), NULL, 0, &ZOO_OPEN_ACL_UNSAFE, 0, \
                       NULL, 0);                                               \
      if (ZOK != ret) {                                                        \
        LOG(ERROR) << " MSG:" << rdp_comm::ErrorStr(ret);                      \
        return rs;                                                             \
      }                                                                        \
    }                                                                          \
  } while (0);

#define CHECKANDSETPATH2(handle, path, value, rs)                    \
  do {                                                               \
    int ret = zoo_exists(handle, path.c_str(), 0, NULL);             \
    if (ZOK != ret) {                                                \
      ret = zoo_create(handle, path.c_str(), value, strlen(value),   \
                       &ZOO_OPEN_ACL_UNSAFE, 0, NULL, 0);            \
      if (ZOK != ret) {                                              \
        LOG(ERROR) << " MSG:" << rdp_comm::ErrorStr(ret);            \
        return rs;                                                   \
      }                                                              \
    } else {                                                         \
      ret = zoo_set(handle, path.c_str(), value, strlen(value), -1); \
      if (ZOK != ret) {                                              \
        LOG(ERROR) << " MSG:" << rdp_comm::ErrorStr(ret);            \
        return rs;                                                   \
      }                                                              \
    }                                                                \
  } while (0);

DECLARE_string(cfg);

using std::string;

namespace rdp_comm {

class RdpEnvironment : public testing::Environment {
 public:
  explicit RdpEnvironment(const string &cfg);
  ~RdpEnvironment();

  virtual void SetUp();
  virtual void TearDown();

  inline AppConfig *GetCfg(void) { return config_; }

  inline zhandle_t *GetZkHandle(void) { return zk_handle_; }

  inline void SetConnect(bool flag) { zk_connected_ = flag; }

 private:
  bool InitConfig(const string &cfg);
  bool InitZk(void);

 private:
  AppConfig *config_;
  string cfg_;
  zhandle_t *zk_handle_;
  bool zk_connected_;
};

void ZkRmR(zhandle_t *zk, string path);

string to_string(int x);

}  // namespace rdp_comm

extern rdp_comm::RdpEnvironment *g_env;

#endif  // RDP_TEST_ENVIRONMENT_H
