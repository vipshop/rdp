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

#ifndef RDP_COMM_ZK_PROCESS_H
#define RDP_COMM_ZK_PROCESS_H

#include "comm_incl.h"
#include "file_mgt.h"

#define ZK_DEFAULT_RECVTIMEOUT 3000
#define CONNECT_WAIT_TIME 10
#define ZK_BUFFER_SIZE 2048

using std::string;
using std::vector;

namespace rdp_comm {

// Zookeeper watch path data callback function
typedef void (*DataWatcherCallback)(const std::string &path,
                                    const std::string &value, void *ctx,
                                    const struct Stat *stat);

// Zookeeper watcg path children callback function
typedef void (*ChildrenWatcherCallback)(const std::string &path,
                                        const std::vector<std::string> &value,
                                        void *ctx, const struct Stat *stat);

// user callback function, call when zookeeper session status change
typedef void (*UserWatcherCallback)(int type, int state, const char *path,
                                    void *data);

// Zookeeper return code class
class ZKRet {
  friend class ZKProcess;

 public:
  // Zookeeper process success
  bool Ok(void) const { return ZOK == code_; }

  // Zookeeper path exists
  bool NodeExists(void) const { return ZNODEEXISTS == code_; }

  // Zookeeper path not exists
  bool NodeNotExists(void) const { return ZNONODE == code_; }

  // Check process result
  operator bool() { return Ok(); }

 protected:
  explicit ZKRet(void) { code_ = ZOK; }
  explicit ZKRet(int ret) { code_ = ret; }

 private:
  int code_;
};

class ZKProcess {
 public:
  explicit ZKProcess(void);

  ~ZKProcess();

  bool Initialize(const string &hosts, const uint32_t recv_time,
                UserWatcherCallback user_func, void *user_ctx,
                const string &zk_log_dir, const string &zk_log_name,
                size_t zk_log_roll_size, int zk_log_days);

  // Connect to zookeeper
  // return true not mean had connected to zookeeper,
  // just success send connect request to zookeeper
  // call IsConnected check whether connected to zookeeper
  bool Connect(void);

  // close connection to zookeeper
  void DisConnect(void);

  // Create path in zookeeper
  // parm: recursive, if true, create recursive path in zookeeper
  ZKRet CreateNode(const string &path, const string &value, bool recursive);

  // Create ZOO_EPHEMERAL and ZOO_SEQUENCE path
  ZKRet CreateEphAndSeqNode(const string &path, const string &value,
                            string &rpath);

  // Get path value
  bool GetData(const string &path, string &value, struct Stat *stat = NULL);

  // Set path value
  bool SetData(const string &path, const string &value,
               struct Stat *stat = NULL, uint32_t version = -1);

  // Check path exists
  bool Exists(const string &path, struct Stat *stat = NULL);

  // Get all childrens path
  bool GetChildren(const string &path, std::vector<string> &children,
                   struct Stat *stat = NULL);

  // watch path value, if change, will callback
  bool WatchData(const string &path, DataWatcherCallback cb, 
                      void *cb_ctx, bool immediately = false);

  // wathc children, if change, will callback
  bool WatchChildren(const string &path, ChildrenWatcherCallback cb,
                      void *cb_ctx, bool immediately = false);

  // synchronous data
  bool SyncPath(const string &path);

  // Set zookeeper log level
  inline void SetLogLevel(ZooLogLevel level) {
    this->zk_log_level_ = level;

    lock_guard guard(&lock_);
    zoo_set_debug_level(this->zk_log_level_);
  }

  // Get zookeeper hosts
  inline string GetHosts(void) { return hosts_; }

  // Check whether had connected to zookeeper
  inline bool IsConnected(void) { return connected_; }

  // Reconnect to zookeeper
  bool Restart(void);

  // if zookeeper state change will call user func
  inline void SetUserFuncAndCtx(UserWatcherCallback user_func, void *user_ctx) {
    this->user_func_ = user_func;
    this->user_ctx_ = user_ctx;
  }

  // get zookeeper session expired timeout time
  inline int GetRecvTimeout(void) {
    return zk_handle_ ? zoo_recv_timeout(zk_handle_) : -1;
  }

  inline FileMgt* GetZKLog(void) {
    return zk_log_;
  }

  static void ZKLog(const char* message);

 private:
  // Call the user_func_ function
  void CallUserCallback(int type, int state, const char *path) const {
    if (NULL != user_func_) user_func_(type, state, path, user_ctx_);
  }

  // watch zookeeper event
  static void DefaultWatcher(zhandle_t *handle, int type, int state,
                             const char *path, void *watcherCtx);

  // use in watch data
  static void DataCompletion(int rc, const char *value, int valuelen,
                             const struct Stat *stat, const void *data);

  // use in watch children
  static void ChildrenCompletion(int rc, const struct String_vector *strings,
                                 const struct Stat *stat, const void *data);

  // use in sync data
  static void SyncCompletion(int rc, const char *value, const void *data);

  // when recv connect response, set had connected zookeeper
  inline void SetConnected(bool connect) { this->connected_ = connect; }

  // Create node
  ZKRet CreateNode2(const string &path, const string &value, int flag,
                    char *rpath, int rpathlen, bool recursive);

  // Set session client id
  void SetClientId(const clientid_t *id);

  // Check exit
  bool IsExit(void) { return exit_flag_; }

  // Abstract watch of DataWatch and ChildrenWatch
  class Watcher {
   public:
    Watcher(ZKProcess *zk, const string &path);
    virtual ~Watcher() {}

    // Set async watch function
    virtual bool Register(void) const = 0;
    virtual bool RegisterAndCall(void) const = 0;

    // Get watch path
    const string &GetPath(void) const { return path_; }

    // Get ZKProcess handle
    ZKProcess *GetZk(void) const { return zk_; }

   protected:
    ZKProcess *zk_;

    // watch path
    string path_;
  };

  // Watch path data
  class DataWatcher : public Watcher {
   public:
    typedef DataWatcherCallback Callback;

   public:
    DataWatcher(ZKProcess *zk, const string &path, const Callback cb,
                void *cb_ctx);

    // Set async watch get data callback
    virtual bool Register(void) const;
    virtual bool RegisterAndCall(void) const;

    // when data change, callback
    void DoCallback(const string &data, const struct Stat *stat) const {
      cb_(path_, data, cb_ctx_, stat);
    }

   private:
    Callback cb_;
    void *cb_ctx_;
  };

  // Watch path children
  class ChildrenWatcher : public Watcher {
   public:
    typedef ChildrenWatcherCallback Callback;

   public:
    ChildrenWatcher(ZKProcess *zk, const string &path, const Callback cb,
                    void *cb_ctx);

    // Set async watch get data callback
    virtual bool Register(void) const;
    virtual bool RegisterAndCall(void) const;

    // when children dir change, callback
    void DoCallback(const vector<string> &data,
                    const struct Stat *stat) const {
      cb_(path_, data, cb_ctx_, stat);
    }

   private:
    Callback cb_;
    void *cb_ctx_;
  };

  // Store all data watchs and children watchs
  class WatcherPool {
    typedef std::map<string, Watcher *> WatcherMap;

   public:
    explicit WatcherPool() {}
    ~WatcherPool() {
      for (WatcherMap::iterator it = watcher_map_.begin();
           it != watcher_map_.end(); ++it) {
        delete it->second;
      }
      watcher_map_.clear();
    }

    // Create DataWatch or ChildrenWatch
    template <class T>
    Watcher *CreateWatcher(ZKProcess *zk, const string &path,
                           typename T::Callback cb, void *cb_ctx) {
      string name = typeid(T).name() + path;
      WatcherMap::iterator it = watcher_map_.find(name);

      if (watcher_map_.end() == it) {

        Watcher *w = new T(zk, path, cb, cb_ctx);
        if (NULL == w) {
          LOG(ERROR) << "New " << typeid(T).name() << ", ERROR: " << errno
                     << ", " << GetLastErrorMsg(errno);
          return NULL;
        }

        watcher_map_[name] = w;
        return w;
      } else {
        return it->second;
      }
    }

    // Get DataWatch or ChildrenWatch
    template <class T>
    Watcher *GetWatcher(const string &path) {
      string name = typeid(T).name() + path;

      WatcherMap::iterator it = watcher_map_.find(name);
      if (watcher_map_.end() == it) {
        return NULL;
      } else {
        return it->second;
      }
    }

    /*
    // Call all watchers
    void GetAll(void) const {
      for (WatcherMap::const_iterator it = watcher_map_.begin();
           it != watcher_map_.end(); ++it) {
        it->second->Get();
      }
    }
    */

   private:
    WatcherMap watcher_map_;
  };

  // zookeeper host list
  string hosts_;

  // true if had connected to zookeeper
  bool connected_;

  // set by configure file
  uint32_t recv_timeout_;

  // zookeeper handle
  zhandle_t *zk_handle_;

  // Zookeeper log level
  ZooLogLevel zk_log_level_;

  // Zookeeper all watchers
  WatcherPool watcher_pool_;

  // session client id
  clientid_t client_id_;

  // Zookeeper status change callback function
  UserWatcherCallback user_func_;

  // Zookeeper status change callback function param
  void *user_ctx_;

  spinlock_t lock_;

  // Exit flag, if true, not reconnect zookeeper
  volatile bool exit_flag_;

  FileMgt *zk_log_;

  // zookeeper c client log file dir
  string zk_log_dir_;

  // zookeeper c client log file name
  string zk_log_name_;

  // zookeeper c client log roll size
  size_t zk_log_roll_size_;

  // zookeeper c client log save days
  int zk_log_days_;
};

}  // namespace rdp_comm

#endif  // RDP_COMM_ZK_PROCESS_H
