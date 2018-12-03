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
#include "rdp_environment.h"

namespace rdp_comm {

static void DefaultWatcher(zhandle_t *handle, int type, int state,
                           const char *path, void *watcherCtx) {
  RdpEnvironment *rdp_env = static_cast<RdpEnvironment *>(watcherCtx);
  assert(NULL != rdp_env);
  if (ZOO_SESSION_EVENT == type) {
    if (ZOO_CONNECTED_STATE == state) {
      LOG(INFO) << "Connected To Zookeeper";
      rdp_env->SetConnect(true);
    } else if (ZOO_EXPIRED_SESSION_STATE == state) {
      LOG(INFO) << "Zookeeper Session Expired";
      rdp_env->SetConnect(false);
    } else {
      LOG(INFO) << "Lost Connection To Zookeeper";
      rdp_env->SetConnect(false);
    }
  } else if (ZOO_CREATED_EVENT == type) {
    LOG(INFO) << "Create Event:" << path;
  } else if (ZOO_DELETED_EVENT == type) {
    LOG(INFO) << "Delete Event:" << path;
  } else if (ZOO_CHANGED_EVENT == type) {
    LOG(INFO) << "Changed Event:" << path;
  } else if (ZOO_CHILD_EVENT == type) {
    LOG(INFO) << "Children Event:" << path;
  } else {
    LOG(ERROR) << "Unknow Zookeeper Event";
    assert(0);
  }
}

RdpEnvironment::RdpEnvironment(const string &cfg) : testing::Environment() {
  cfg_ = cfg;
  config_ = NULL;
}

RdpEnvironment::~RdpEnvironment() {
  if (config_) {
    delete config_;
    config_ = NULL;
  }
}

void RdpEnvironment::SetUp() {
  if (!InitConfig(cfg_)) {
    LOG(ERROR) << "Init Configure File:" << cfg_ << " Failt";
    exit(-1);
  }

  ZKProcess *zk_process = &Singleton<ZKProcess>::GetInstance();
  assert(NULL != zk_process);
  string zk_hosts = config_->GetValue<string>("zk.hosts");
  int32_t zk_recv_timeout = config_->GetValue<int32_t>("zk.recvtimeout");
  string zk_log_level = config_->GetValue<string>("zk.loglevel");


  // Zookeeper log dir
  string log_dir = config_->GetValue<string>(string("log.dir"));

  // Zookeeper log, not use google log
  string log_name = config_->GetValue<string>(string("zk.log.name"));
  size_t log_size = config_->GetValue<size_t>("zk.log.size", 1024 * 1024 * 128);
  int log_days = config_->GetValue<int>("zk.log.days", 7);

  assert(true == zk_process->Initialize(zk_hosts, zk_recv_timeout, NULL, NULL, log_dir, log_name, log_size, log_days));

  // Connect Zookeeper
  assert(true == zk_process->Connect());

  assert(true == InitZk());
}

void RdpEnvironment::TearDown() {
  ZKProcess *zk_process = &Singleton<ZKProcess>::GetInstance();
  assert(NULL != zk_process);
  zk_process->DisConnect();

  if (zk_handle_) {
    zookeeper_close(zk_handle_);
    zk_handle_ = NULL;
  }
}

bool RdpEnvironment::InitConfig(const string &cfg) {
  if (NULL == config_) {
    config_ = new AppConfig(cfg);
    if (NULL == config_) {
      LOG(ERROR) << "New AppConfig Failt:" << strerror(errno);
      return false;
    }
  }
  return true;
}

bool RdpEnvironment::InitZk() {
  string hosts = config_->GetValue<string>("zk.hosts");
  int32_t recv_timeout = config_->GetValue<int32_t>("zk.recvtimeout");
  zk_handle_ = zookeeper_init(hosts.c_str(), DefaultWatcher, recv_timeout, NULL,
                              this, 0);
  if (NULL == zk_handle_) {
    LOG(ERROR) << "Zookeeper Init Failt";
    return false;
  }
  string log_level = config_->GetValue<string>("zk.loglevel");
  zoo_set_debug_level(log_level.compare("ERROR") == 0
                          ? ZOO_LOG_LEVEL_ERROR
                          : log_level.compare("WARN") == 0
                                ? ZOO_LOG_LEVEL_WARN
                                : log_level.compare("INFO") == 0
                                      ? ZOO_LOG_LEVEL_INFO
                                      : log_level.compare("DEBUG") == 0
                                            ? ZOO_LOG_LEVEL_DEBUG
                                            : ZOO_LOG_LEVEL_WARN);

  for (int i = 0; i < 10; ++i) {
    if (zk_connected_) goto end;
    LOG(INFO) << "Wait For Connecting To Zookeeper";
    sleep(1);
  }
  return false;
end:
  string zk_path = config_->GetValue<string>("zk.path");
  int ret = zoo_exists(zk_handle_, zk_path.c_str(), 0, NULL);
  if (ZOK == ret) {
    ZkRmR(zk_handle_, zk_path);
  }
  return true;
}

void ZkRmR(zhandle_t *zk, string path) {
  String_vector sv;
  int ret = zoo_get_children(zk, path.c_str(), 0, &sv);
  if (ZOK != ret) {
    LOG(ERROR) << "zoo_get_children:" << path
               << " error:" << rdp_comm::ErrorStr(ret);
    return;
  }
  for (int i = 0; i < sv.count; ++i) {
    if (0 == path.compare("/")) {
      ZkRmR(zk, string(path + sv.data[i]));
    } else {
      ZkRmR(zk, string(path + "/" + sv.data[i]));
    }
  }
  ret = zoo_delete(zk, path.c_str(), -1);
  if (ZOK != ret) {
    LOG(ERROR) << "Delete Path:" << path
               << " Failt:" << rdp_comm::ErrorStr(ret);
    return;
  } else {
    LOG(INFO) << "Delete Path:" << path;
  }
  deallocate_String_vector(&sv);
}

string to_string(int x) {
  static char buf[56] = {0x0};
  snprintf(buf, sizeof(buf) - 1, "%d", x);
  return string(buf, strlen(buf));
}

}  // namespace rdp_comm

rdp_comm::RdpEnvironment *g_env;
