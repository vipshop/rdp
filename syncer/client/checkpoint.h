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

#ifndef _RDP_CHECKPOINT_H_
#define _RDP_CHECKPOINT_H_

#define __STDC_FORMAT_MACROS
#include <inttypes.h>

#include <rdp-comm/threadbase.h>
#include <rdp-comm/zk_process.h>
#include <rdp-comm/signal_handler.h>
#include <rdp-comm/kafka_consumer.h>
#include "syncer_utils.h"
#include "syncer_incl.h"
#include "rdp.pb.h"
#include "mysql_gtidmgr.h"
#include "jansson_pack.h"
#include "split_package.h"

using std::string;
using std::vector;
using std::queue;
using rdp_comm::ZKProcess;
using rdp_comm::ThreadBase;

namespace syncer {

class CheckPoint : public ThreadBase {
 private:
  enum DefIntervalMs {
    DEF_INTERVAL_MS = 3000
  };

 public:
  CheckPoint();
  ~CheckPoint();

  // \brief Init
  //
  // \param brokerlist
  // \param topic
  // \param partition
  // \param fetch_max_msg_bytes
  // \param zk_path
  // \param interval_ms
  //
  // \return
  bool Init(const string &brokerlist, const string &topic,
            ZKProcess *zk_handle, const string &zk_path,
            const uint32_t interval_ms, const uint32_t release_ms);

  // \brief AddGtid: add gtid text to MySQLGtidMgr
  //
  // \param gtid
  //
  // \return
  bool AddGtid(const string &gtid);


  // \brief WaitGtid: wait gtid until acked by kafka
  //
  // \param gtid
  //
  // \return
  void WaitGtidAcked(const string &gtid);

  // \brief GetGtidSet: get MySQLGtidMgr text
  //
  // \return
  string GetGtidSet(void);

  // \brief UpdateOffset: update the topic offset.
  //
  // \param offset
  //
  // \return
  void UpdateOffset(const uint64_t offset) {
    mutex_guard guard(&lock_);
    topic_offset_ = offset;
    update_flag_ = true;
  }

  uint64_t GetOffset(void) {
    return topic_offset_;
  }

  inline void Exit(void) { exit_flag_ = true; }

  uint64_t GetEpoch(void) {
    return epoch_;
  }

  void SetEpoch(uint64_t epoch) {
    mutex_guard guard(&lock_);
    epoch_ = epoch;
    update_flag_ = true;
  }

  uint64_t GetTransSeqNo(void) {
    return trans_seq_no_;
  }

  void SetTransSeqNo(uint64_t seq_no) {
    mutex_guard guard(&lock_);
    // 如果kafka leader切换，可能出现乱序，message callback也会乱序，导致transaction seq no不是单调+1递增
    if (seq_no != (trans_seq_no_ + 1)) {
      RDP_LOG_ERROR << "CheckPoint Update transa_seq_no[" << seq_no << "] != " << "curr trans_seq_no[" << trans_seq_no_<< "] + 1";
      abort();
    }
    trans_seq_no_ = seq_no;
    update_flag_ = true;
  }

  void PushSeqKafkaPkg(SeqKafkaPkg *pkg);

  void SetZkFlag(const string &flag) {
    zk_flag_ =flag;
  }

  string GetZkFlag(void) {
    mutex_guard guard(&lock_);
    return zk_flag_;
  }
 protected:
  void Run(void);

 private:
  // Save checkpoint to zookeeper
  void SaveCheckPoint(const std::string&);

  void ProcSeqKafkaPkg(void);

 private:
  ZKProcess *zk_handle_;
  // update gtid_mgr and offset lock
  mutex_t lock_;
  // condition to signal specified gitd has been ack by kafka
  cond_t cond_;
  string target_gtid_;
  // gtid set
  MySQLGtidMgr gtid_mgr_;
  // checkpoint interval time
  uint32_t interval_ms_;
  // kafka brokerlist
  string brokerlist_;
  // kafka topic
  string topic_name_;
  // topic offset
  uint64_t topic_offset_;
  // zookeeper path store checkpont info
  string zk_path_;
  // the version of the zookeeper node
  int32_t node_version_;
  // exit thread
  volatile bool exit_flag_;
  // kafka epoch
  uint64_t epoch_;
  // kafka transaction seq no
  uint64_t trans_seq_no_;
  // write kafka response cache lock 
  pthread_mutex_t wait_update_pkgs_mutex_;
  // write kafka response cache
  queue<SeqKafkaPkg*> wait_update_pkgs_;
  //string kafka_debug_;
  string zk_flag_;
  //checkpoint info updated flag
  bool update_flag_;
  // checkpoint release memory interval time
  uint32_t release_ms_;
};
}  // namespace syncer

#endif  // _RDP_CHECKPOINT_H_
