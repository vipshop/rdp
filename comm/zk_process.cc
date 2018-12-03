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

#include "zk_process.h"

namespace rdp_comm {

ZKProcess::ZKProcess()
    : zk_handle_(NULL),
      connected_(false),
      user_func_(NULL),
      user_ctx_(NULL),
      exit_flag_(false),
      zk_log_roll_size_(1024 * 1024 * 128),
      zk_log_days_(7),
      zk_log_(NULL) {
  recv_timeout_ = ZK_DEFAULT_RECVTIMEOUT;
  zk_log_level_ = ZOO_LOG_LEVEL_WARN;
  memset(&client_id_, 0, sizeof(clientid_t));
}

ZKProcess::~ZKProcess() {
  if (NULL != zk_handle_) {
    zookeeper_close(zk_handle_);
    zk_handle_ = NULL;
  }

  if (zk_log_) {
    delete zk_log_;
    zk_log_ = NULL;
  }
}

bool ZKProcess::Initialize(const string &hosts, const uint32_t recv_time,
                         UserWatcherCallback user_func, void *user_ctx,
                         const string &zk_log_dir, const string &zk_log_name,
                         size_t zk_log_roll_size, int zk_log_days) {
  hosts_ = hosts;
  zk_log_dir_ = zk_log_dir;
  zk_log_name_ = zk_log_name;
  zk_log_roll_size_ = zk_log_roll_size;
  zk_log_days_ = zk_log_days;
  recv_timeout_ = recv_time;

  memset(&client_id_, 0, sizeof(clientid_t));

  zk_log_ = new FileMgt(zk_log_dir_, zk_log_name_, ".log", zk_log_roll_size_,
                        zk_log_days_);

  if (zk_log_ == NULL) {
    return false;
  } else {
    return true;
  }
}

bool ZKProcess::Connect() {
  if (hosts_.empty()) {
    LOG(ERROR) << "Connect Hosts Is Empty";
    return false;
  }

  if (NULL != zk_handle_) return true;

  lock_guard guard(&lock_);

  // set zookeeper c client file stream to save log info when zookeeper_init
  zoo_set_log_stream(zk_log_->GetFileHandle());

  // set default log level
  zoo_set_debug_level(this->zk_log_level_);

  zk_handle_ = zookeeper_init(hosts_.c_str(), DefaultWatcher, recv_timeout_,
                              &client_id_, this, 0);
  if (NULL == zk_handle_) {
    LOG(ERROR) << "Connect to Zookeeper: " << hosts_ << " Failt";
    return false;
  }

  // set zookeeper log callback function 
  zoo_set_log_callback(zk_handle_, &ZKProcess::ZKLog);
  zoo_set_global_zh(zk_handle_);

  // wait for zookeeper connected callback
  for (int i = 0; i < CONNECT_WAIT_TIME; ++i) {
    LOG(INFO) << "Sleep 1 Second Wait For Zookeeper Response";
    sleep(1);
    if (connected_) {
      goto end;
    }
  }

  LOG(ERROR) << "Connect to Zookeeper: " << hosts_
             << ", Failt. Not Response After: " << CONNECT_WAIT_TIME
             << " Second";

  zookeeper_close(zk_handle_);
  memset(&client_id_, 0, sizeof(clientid_t));
  zk_handle_ = NULL;

  return false;

end:
  return true;
}

void ZKProcess::DisConnect(void) {
  if (NULL == zk_handle_) return;
  lock_guard guard(&lock_);

  exit_flag_ = true;

  if (connected_) {
    zookeeper_close(zk_handle_);
    memset(&client_id_, 0, sizeof(clientid_t));
  }

  zk_handle_ = NULL;
  connected_ = false;
  LOG(INFO) << "Disconnect To Zookeeper: " << hosts_;
  return;
}

ZKRet ZKProcess::CreateNode(const string &path, const string &value,
                            bool recurisive) {
  lock_guard guard(&lock_);
  return CreateNode2(path, value, 0, NULL, 0, recurisive);
}

ZKRet ZKProcess::CreateEphAndSeqNode(const string &path, const string &value,
                                     string &rpath) {
  lock_guard guard(&lock_);

  // Sync path
  if (!SyncPath(path)) {
    LOG(ERROR) << "Sync Zookeeper Path: " << path << " Failt";
    return ZKRet(ZSYSTEMERROR);
  }

  char buffer[ZK_BUFFER_SIZE] = {0};

  // must append '/' to node_dir
  // because CreateNode2 will create node in node_dir+'0000000001'
  // so if not append '/', it will not create node in dir node_dir
  // eg: node_dir='/a/b', not append '/', will create '/a/b0000000001' node
  string node_dir = path;
  if (node_dir.c_str()[node_dir.length() - 1] != '/') {
    node_dir.append("/");
  }

  ZKRet zret = CreateNode2(node_dir, value, ZOO_EPHEMERAL | ZOO_SEQUENCE, buffer,
                           sizeof(buffer), false);
  rpath = buffer;
  return zret;
}

// recursive create dir in zookeeper if recursive is true
ZKRet ZKProcess::CreateNode2(const string &path, const string &value, int flag,
                             char *rpath, int rpathlen, bool recursive) {
  assert(!(0 != (ZOO_SEQUENCE & flag) && rpath == NULL));

  int ret = zoo_create(zk_handle_, path.c_str(), value.c_str(), value.length(),
                       &ZOO_OPEN_ACL_UNSAFE, flag, rpath, rpathlen);

  if (ZNONODE == ret) {
    if (!recursive) {
      return ZKRet(ret);
    }

    string ppath = ParentPath(path);

    if (ppath.empty()) {
      LOG(ERROR) << "Recursive CreateNode Parent Path: " << ppath
                 << ", Is Empty";
      return ZKRet(ret);
    }

    ZKRet zret = CreateNode2(ppath.c_str(), "", 0, NULL, 0, true);

    if (zret.Ok() || zret.NodeExists()) {
      ret = zoo_create(zk_handle_, path.c_str(), value.c_str(), value.length(),
                       &ZOO_OPEN_ACL_UNSAFE, flag, rpath, rpathlen);
      if (ZOK != ret && ZNODEEXISTS != ret) {
        LOG(ERROR) << "Create Node: " << path << ", Failt: " << ErrorStr(ret);
      }
      return ZKRet(ret);
    } else {
      return zret;
    }
  } else if (ZOK != ret && ZNODEEXISTS != ret) {
    LOG(ERROR) << "Create Node: " << path << ", Failt: " << ErrorStr(ret);
  }
  return ZKRet(ret);
}

bool ZKProcess::GetData(const string &path, string &value,
                        struct Stat *stat) {
  char buffer[ZK_BUFFER_SIZE] = {0};
  int buffer_size = sizeof(buffer);

  lock_guard guard(&lock_);

  int ret = zoo_get(zk_handle_, path.c_str(), 0, buffer, &buffer_size, stat);
  if (ZOK != ret) {
    LOG(ERROR) << "Get Path: " << path << ", Failt: " << ErrorStr(ret);
    return false;
  } else {
    value = buffer;
    return true;
  }
}

bool ZKProcess::SetData(const string &path, const string &value,
                        struct Stat *stat, uint32_t version) {
  int ret = 0;

  // release lock_
  {
    lock_guard guard(&lock_);
    ret = zoo_set2(zk_handle_, path.c_str(), value.c_str(), value.length(), version,
                   stat);
  }

  if (ZOK != ret) {
    // if not exists, create dir
    if (ZNONODE == ret) {
      return CreateNode(path, value, true);
    } else {
      LOG(ERROR) << "Set Path: " << path << ", Value: " << value
                 << ", Failt: " << ErrorStr(ret);
      return false;
    }
  } else {
    return true;
  }
}

bool ZKProcess::Exists(const string &path, struct Stat *stat) {
  lock_guard guard(&lock_);
  if (NULL == zk_handle_)
    return true;
  return (ZOK == zoo_exists(zk_handle_, path.c_str(), 0, stat));
}

bool ZKProcess::GetChildren(const string &path, std::vector<string> &children,
                            struct Stat *stat) {
  String_vector sv;

  lock_guard guard(&lock_);

  // zoo_get_children2 not check stat is NULL
  int ret = 0;
  if (NULL != stat) {
    ret = zoo_get_children2(zk_handle_, path.c_str(), 0, &sv, stat);
  } else {
    ret = zoo_get_children(zk_handle_, path.c_str(), 0, &sv);
  }
  if (ZOK != ret) {
    LOG(ERROR) << "Get Children: " << path << ", Failt: " << ErrorStr(ret);
    return false;
  } else {
    for (int i = 0; i < sv.count; ++i) {
      children.push_back(sv.data[i]);
    }
    deallocate_String_vector(&sv);
    return true;
  }
}

bool ZKProcess::SyncPath(const string &path) {
  if (NULL == zk_handle_ || !IsConnected()) {
    LOG(ERROR) << "Not Connected Zookeeper";
    return false;
  }

  DLOG(INFO) << "Sync Zoopkeeper Path: " << path;

  int rc = -999;
  int ret =
      zoo_async(zk_handle_, path.c_str(), &ZKProcess::SyncCompletion, &rc);

  if (ZOK != ret) {
    LOG(ERROR) << "Zoo_async Failt: " << ErrorStr(ret);
    return false;
  }

  // Wait the timeout for this session
  for (int i = 0; i < GetRecvTimeout() / 10; ++i) {
    if (-999 == rc) {
      DLOG(INFO) << "Wait Zoo_async Callback. Sleep 10 ms";
      usleep(10 * 1000);
    }
  }

  if (-999 == rc) {
    LOG(ERROR) << "Wait: " << GetRecvTimeout() << " ms Zoo_async Not Callback";
    return false;
  } else {
    return true;
  }
}

void ZKProcess::DefaultWatcher(zhandle_t *handle, int type, int state,
                               const char *path, void *watcherCtx) {
  ZKProcess *zk = static_cast<ZKProcess *>(watcherCtx);

  if (ZOO_SESSION_EVENT == type) {
    if (ZOO_CONNECTED_STATE == state) {
      zk->SetConnected(true);
      zk->SetClientId(zoo_client_id(handle));
      LOG(INFO) << "Connected To Zookeeper: " << zk->GetHosts()
                << ", Seesion State: " << StateStr(state);

    } else if (ZOO_EXPIRED_SESSION_STATE == state) {
      // TODO: Need handle it
      LOG(ERROR) << "The session expired, we don't kown how to recover, exit now";
      exit(1);

    } else {
      zk->SetConnected(false);
      LOG(ERROR) << "Not Connect To Zookeeper: " << zk->GetHosts()
                 << ", Seesion State: " << StateStr(state);

      // reconnect zookeeper
    }

  } else if (ZOO_CREATED_EVENT == type) {
    DLOG(INFO) << "Create Node: " << path;

  } else if (ZOO_DELETED_EVENT == type) {
    DLOG(INFO) << "Delete Node: " << path;

  } else if (ZOO_CHANGED_EVENT == type) {
    DLOG(INFO) << "Changed Node: " << path;
    zk->watcher_pool_.GetWatcher<DataWatcher>(path)->RegisterAndCall();

  } else if (ZOO_CHILD_EVENT == type) {
    DLOG(INFO) << "Children Node: " << path;
    zk->watcher_pool_.GetWatcher<ChildrenWatcher>(path)->RegisterAndCall();

  } else {
    LOG(WARNING) << "Unhandled Zookeeper Event: " << EventStr(type);
  }

  zk->CallUserCallback(type, state, path);
}

void ZKProcess::DataCompletion(int rc, const char *value, int valuelen,
                               const struct Stat *stat, const void *data) {
  const DataWatcher *watcher =
      dynamic_cast<const DataWatcher *>(static_cast<const Watcher *>(data));

  if (ZOK == rc) {
    watcher->DoCallback(string(value, valuelen), stat);
  } else {
    LOG(ERROR) << "DataCompletion Path: " << watcher->GetPath()
               << ", Failt: " << ErrorStr(rc);
  }
}

void ZKProcess::ChildrenCompletion(int rc, const struct String_vector *strings,
                                   const struct Stat *stat, const void *data) {
  const ChildrenWatcher *watcher =
      dynamic_cast<const ChildrenWatcher *>(static_cast<const Watcher *>(data));

  if (ZOK == rc) {
    vector<string> vc;
    for (int i = 0; i < strings->count; ++i) {
      vc.push_back(strings->data[i]);
    }

    watcher->DoCallback(vc, stat);
  } else {
    LOG(ERROR) << "ChildrenCompletion Path: " << watcher->GetPath()
               << ", Failt: " << ErrorStr(rc);
  }
}

void ZKProcess::SyncCompletion(int rc, const char *value, const void *data) {
  assert(NULL != data);
  int *ret = static_cast<int *>(const_cast<void *>(data));
  *ret = rc;
}

bool ZKProcess::WatchData(const string &path, DataWatcherCallback cb,
                          void *cb_ctx, bool immediately) {
  if (!connected_) {
    LOG(ERROR) << "Not Connect To Zookeeper";
    return false;
  }

  if (!Exists(path)) {
    LOG(ERROR) << "Path: " << path << ", Not Exists In Zookeeper";
    return false;
  }

  Watcher *w = watcher_pool_.CreateWatcher<DataWatcher>(this, path, cb, cb_ctx);

  return immediately ? w->RegisterAndCall() : w->Register();
}

bool ZKProcess::WatchChildren(const string &path, ChildrenWatcherCallback cb,
                              void *cb_ctx, bool immediately) {
  if (!connected_) {
    LOG(ERROR) << "Not Connect To Zookeeper";
    return false;
  }

  if (!Exists(path)) {
    LOG(ERROR) << "Watch Children: " << path << ", Not Exists";
    return false;
  }

  Watcher *w =
      watcher_pool_.CreateWatcher<ChildrenWatcher>(this, path, cb, cb_ctx);

  return immediately ? w->RegisterAndCall() : w->Register();
}

bool ZKProcess::Restart() {
  // TODO: Implement the restart logic: recover ephemeral 
  // nodes and watchers
  assert(0);
  return true;
}

ZKProcess::Watcher::Watcher(ZKProcess *zk, const string &path)
    : zk_(zk), path_(path) {}

ZKProcess::DataWatcher::DataWatcher(ZKProcess *zk, const string &path,
                                    const Callback cb, void *cb_ctx)
    : Watcher(zk, path), cb_(cb), cb_ctx_(cb_ctx) {}

bool ZKProcess::DataWatcher::Register() const {
  struct Stat stat;
  int ret =
      zoo_wget(zk_->zk_handle_, path_.c_str(), &ZKProcess::DefaultWatcher,
                this->GetZk(), NULL, NULL, &stat);
  if (ZOK != ret) {
    LOG(ERROR) << "Zoo_awget Path: " << path_ << ", Failt: " << ErrorStr(ret);
    return false;
  }
  return true;
}

bool ZKProcess::DataWatcher::RegisterAndCall() const {
  int ret =
      zoo_awget(zk_->zk_handle_, path_.c_str(), &ZKProcess::DefaultWatcher,
                this->GetZk(), &ZKProcess::DataCompletion, this);
  if (ZOK != ret) {
    LOG(ERROR) << "Zoo_awget Path: " << path_ << ", Failt: " << ErrorStr(ret);
    return false;
  }
  return true;
}

ZKProcess::ChildrenWatcher::ChildrenWatcher(ZKProcess *zk, const string &path,
                                            const Callback cb, void *cb_ctx)
    : Watcher(zk, path), cb_(cb), cb_ctx_(cb_ctx) {}

bool ZKProcess::ChildrenWatcher::Register() const {
  struct String_vector strings;
  struct Stat stat;
  int ret = zoo_wget_children2(zk_->zk_handle_, path_.c_str(),
                                &ZKProcess::DefaultWatcher, this->GetZk(),
                                &strings, &stat);
  if (ZOK != ret) {
    LOG(ERROR) << "Zoo_awget_children Path: " << path_
               << ", Failt: " << ErrorStr(ret);
    return false;
  }

  return true;
}

bool ZKProcess::ChildrenWatcher::RegisterAndCall() const {
  int ret = zoo_awget_children2(zk_->zk_handle_, path_.c_str(),
                                &ZKProcess::DefaultWatcher, this->GetZk(),
                                &ZKProcess::ChildrenCompletion, this);
  if (ZOK != ret) {
    LOG(ERROR) << "Zoo_awget_children Path: " << path_
               << ", Failt: " << ErrorStr(ret);
    return false;
  }

  return true;
}

void ZKProcess::SetClientId(const clientid_t *id) {
  if (NULL != id &&
      (client_id_.client_id == 0 || client_id_.client_id != id->client_id)) {
    memcpy(&client_id_, id, sizeof(clientid_t));
  }
}

void ZKProcess::ZKLog(const char *message) {
  static ZKProcess *zk_handle =
      &rdp_comm::Singleton<rdp_comm::ZKProcess>::GetInstance();
  FileMgt *log = zk_handle->GetZKLog();
  if (log) {
    char buf[2048];
    size_t len = snprintf(buf, sizeof(buf) - 1, "%s\n", message);
    log->Append(buf, len);
  } else {
    LOG(INFO) << "Zookeeper Log: " << message;
  }
}

}  // namespace rdp_comm
