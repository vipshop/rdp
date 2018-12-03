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

#include "checkpoint.h"

#include <rdp-comm/fiu_engine.h>
#include <rdp-comm/leader_election.h>

#include "split_package.h"
#include "syncer_app.h"
#include "rdp_syncer_metric.h"

using rdp_comm::LeaderElection;

namespace syncer {

static void ThreadCleanupHandler(void *args) {}

CheckPoint::CheckPoint() {
  zk_handle_ = NULL;
  interval_ms_ = DEF_INTERVAL_MS;
  topic_offset_ = 0;
  exit_flag_ = false;
  node_version_ = 0;
  epoch_ = 0;
  trans_seq_no_ = 0;
  //第一次启动时必须保存一个checkpoint信息，因为启动时checkpoint可能为空，
  //全量同步binlog数据，在下次写checkpoint前，如果rdp异常，rdp再次启动将继续全量同步binlog数据
  update_flag_ = true;

  PthreadCall("pthread_mutex_init",
              pthread_mutex_init(&wait_update_pkgs_mutex_, NULL));
}

CheckPoint::~CheckPoint() {
  PthreadCall("pthread_mutex_destroy",
              pthread_mutex_destroy(&wait_update_pkgs_mutex_));
}

bool CheckPoint::Init(const string &brokerlist, const string &topic,
                      rdp_comm::ZKProcess *zk_handle, const string &zk_path,
                      const uint32_t interval_ms, const uint32_t release_ms) {
  assert(NULL != zk_handle);

  string read_err;

  string zk_timestamp;
  string zk_offset;
  // default zk_flag "0", read message from kafka
  string zk_flag = "0";
  string zk_topic;
  string zk_gtid;
  string zk_epoch;
  string zk_seq_no;

  brokerlist_ = brokerlist;
  topic_name_ = topic;
  zk_handle_ = zk_handle;
  zk_path_ = zk_path;
  interval_ms_ = interval_ms;
  release_ms_ = release_ms;

  topic_offset_ = 0;

  // get checkpoint store in zookeeper
  struct Stat stat;
  string checkpoint_str;
  if (!zk_handle_->GetData(zk_path_, checkpoint_str, &stat)) {
    RDP_LOG_ERROR << "Zookeeper Path: " << zk_path_ << " Not Exists";
    return false;
  }
  node_version_ = stat.version;

  if (!checkpoint_str.empty()) {
    // decode jansson string
    if (!CPJansson::Decode(checkpoint_str, &zk_topic, &zk_offset, &zk_flag,
                           &zk_timestamp, &zk_gtid, &zk_epoch, &zk_seq_no)) {
      RDP_LOG_ERROR << "Decode CheckPoint Jansson Str: " << checkpoint_str
                 << " Failt";
      goto err;
    }

    RDP_LOG_INFO << "Load CheckPoint From Zookeeper, Timestamp: " << zk_timestamp
              << ", Topic Info: " << zk_topic << ", Topic Offset: " << zk_offset
              << ", Flag: " << zk_flag << ", GTID Set: " << zk_gtid
              << ", Epoch:" << zk_epoch << ", Transaction Seq No:" << zk_seq_no;

    if (!zk_offset.empty()) {
      // get topic offset
      if (atoll(zk_offset.c_str()) < 0) {
        RDP_LOG_ERROR << "CheckPoint Offset: " << zk_offset << " <= 0";
        return false;
      }
      topic_offset_ = atoll(zk_offset.c_str());
    }

    if (!zk_epoch.empty()) {
      if (atoll(zk_epoch.c_str()) < 0) {
        RDP_LOG_ERROR << "CheckPoint Epoch: " << zk_epoch << " <= 0";
        return false;
      }
      epoch_ = atoll(zk_epoch.c_str());
    }

    if (!zk_seq_no.empty()) {
      if (atoll(zk_seq_no.c_str()) < 0) {
        RDP_LOG_ERROR << "CheckPoint Transaction Seq No: " << zk_seq_no << " <= 0";
        return false;
      }
      trans_seq_no_ = atoll(zk_seq_no.c_str());
    }
  } else {
    RDP_LOG_WARN << "Zookeeper Path: " << zk_path_ << " Is Empty";
    //如果zk中的checkpoint信息为空，表示需要重新开始同步数据，不需要理会kafka中是否有数据，所以设置zk_flag_为1
    SetZkFlag("1");
    return true;
  }

  if (!zk_gtid.empty()) {
    // init gtidset from zookeeper
    if (!gtid_mgr_.AddText(zk_gtid.c_str())) {
      RDP_LOG_ERROR << "Add GTID Set Text: " << zk_gtid << ", To Gtid Mgr Failt";
      return false;
    }
  }

  SetZkFlag(zk_flag);

  return true;

err:
  return false;
}

void CheckPoint::SaveCheckPoint(const string &checkpoint_str) {
  struct Stat stat;
  if (!zk_handle_->SetData(zk_path_, checkpoint_str, &stat, node_version_)) {
    RDP_LOG_ERROR << "Update Checkpoint failed: " << checkpoint_str;

    g_syncer_app->Alarm("Update Checkpoint failed");
    
    // Check why it failed
    LeaderElection *leader =
        &rdp_comm::Singleton<LeaderElection>::GetInstance();
    if (leader->IsLeader()) {
      if (zk_handle_->IsConnected()) {
        // Network is ok, so maybe someone else is writing
        RDP_LOG_ERROR << "I am leader, but it seems that someone else is writing "
                      "the checkpoint!";
        node_version_ = -1;
      }
    } else {
      RDP_LOG_ERROR << "I am not leader, but writing the checkpoint, exit now";
      exit(1);
    }
    return;
  }

  node_version_ = stat.version;
  RDP_LOG_INFO << "Update Checkpoint succeed: " << checkpoint_str;
  update_flag_ = false;
}

void CheckPoint::Run() {
  RDP_LOG_DBG << "Start CheckPoint Thread";

  PthreadCall("pthread_detach", pthread_detach(pthread_self()));
  pthread_cleanup_push(ThreadCleanupHandler, this);

  rdp_comm::signalhandler::AddThread(pthread_self(), "checkpoint.thd");

  string gtidmsg_str;

  char offset_buffer[32] = {0x0};
  size_t offset_buffer_len = 0;

  char epoch_buffer[32] = {0x0};
  size_t epoch_buffer_len = 0;

  char seq_no_buffer[32] = {0x0};
  size_t seq_no_buffer_len = 0;

  string topic_info;
  string checkpoint_str;
  topic_info = brokerlist_ + "->" + topic_name_;

  struct timeval pre_save_time;
  struct timeval pre_proc_pkgs_time;
  struct timeval now;

  // not init pre_save_time, when checkpoint thread start, save to zookeeper
  // immediately
  gettimeofday(&now, NULL);

  uint64_t cp_sleep_ms = 0;
  uint64_t pkgs_sleep_ms = 0;

  while (!exit_flag_ && rdp_comm::signalhandler::IsRunning()) {
    gettimeofday(&now, NULL);

    // reach checkpoint time
    cp_sleep_ms = rdp_comm::GetIntervalMs(&pre_save_time, &now);
    if (cp_sleep_ms >= interval_ms_) {

      {
        mutex_guard guard(&lock_);
        gtidmsg_str = gtid_mgr_.ToString();

        RDP_LOG_DBG << "Get gtid str: " << gtidmsg_str;

        offset_buffer_len = snprintf(offset_buffer, sizeof(offset_buffer) - 1,
                                     "%" PRIu64, topic_offset_);
        epoch_buffer_len = snprintf(epoch_buffer, sizeof(epoch_buffer) - 1,
                                    "%" PRIu64, epoch_);
        seq_no_buffer_len = snprintf(seq_no_buffer, sizeof(seq_no_buffer) - 1,
                                     "%" PRIu64, trans_seq_no_);
      }

      pre_save_time = now;

      // if gtid set is empty, means that there is no binlog events
      if (!gtidmsg_str.empty()) {

        // if it is this thread created checkpoint, flag must be '0'. Rebuild
        // GtidSet must merge the kafka message gtid
        if (!CPJansson::Encode(
                 topic_info, string(offset_buffer, offset_buffer_len), "0",
                 rdp_comm::StrTime(NULL), gtidmsg_str,
                 string(epoch_buffer, epoch_buffer_len),
                 string(seq_no_buffer, seq_no_buffer_len), &checkpoint_str)) {
          RDP_LOG_WARN << "CheckPoint Encode Topic Info: " << topic_info
                       << ", Offset: "
                       << string(offset_buffer, offset_buffer_len)
                       << ", Flag: 0, Timestamp: " << rdp_comm::StrTime(NULL)
                       << ", GtidSet: " << gtidmsg_str
                       << ", Epoch:" << string(epoch_buffer, epoch_buffer_len)
                       << ", Transaction Seq No:"
                       << string(seq_no_buffer, seq_no_buffer_len);
        } else {
          if (update_flag_) {
            SaveCheckPoint(checkpoint_str);
          } else {
            RDP_LOG_INFO << "Checkpoint Info Not Change: " << checkpoint_str; 
          }
        }
      }
    }

    pkgs_sleep_ms = rdp_comm::GetIntervalMs(&pre_proc_pkgs_time, &now);
    if (pkgs_sleep_ms > release_ms_) {
      ProcSeqKafkaPkg();
      pre_proc_pkgs_time = now;
    }

    // sleep 10ms
    usleep(10 * 1000);

    // fiu check all thread alive
    FIU_EXECUTE_IF("check_thread_alive", {
      RDP_LOG_DBG << "In FIU check_thread_alive Point, Sleep 10s";
      sleep(10);
      return;
    });
  }

  // Quit parsing from signal thread
  rdp_comm::signalhandler::Wait();
  pthread_cleanup_pop(1);

  RDP_LOG_DBG << "CheckPoint Thread Stop";
}

bool CheckPoint::AddGtid(const string &gtid) {
  mutex_guard guard(&lock_);
  if (!gtid_mgr_.IsIncludeForStr(gtid.c_str())) {
    if (!gtid_mgr_.AddText(gtid.c_str())) {
      RDP_LOG_ERROR << "CheckPoint AddGtid: " << gtid << " Failt";
      return false;
    }
  }

  if (!target_gtid_.empty()) {
    // someone is waiting for target_gtid_ to  be acked
    if (gtid_mgr_.IsIncludeForStr(target_gtid_.c_str())) {
      cond_.broadcast();
    }
  }
  return true;
}

void CheckPoint::WaitGtidAcked(const string &gtid) {
  if (gtid.empty()) {
    return;
  }
  mutex_guard guard(&lock_);
  target_gtid_ = gtid;
  while (!gtid_mgr_.IsIncludeForStr(target_gtid_.c_str())) {
    cond_.wait(lock_.get());
  }
  target_gtid_ = "";
}


string CheckPoint::GetGtidSet() {
  mutex_guard guard(&lock_);
  return gtid_mgr_.ToString();
}

void CheckPoint::PushSeqKafkaPkg(SeqKafkaPkg *pkg) {
  PthreadCall("pthread_mutex_lock",
              pthread_mutex_lock(&wait_update_pkgs_mutex_));

  wait_update_pkgs_.push(pkg);
  RDP_LOG_DBG << "CheckPoint Add GTID:" << pkg->trans->gtid
             << ", Key:" << pkg->Key();

  PthreadCall("pthread_mutex_unlock",
              pthread_mutex_unlock(&wait_update_pkgs_mutex_));
}

void CheckPoint::ProcSeqKafkaPkg(void) {
  vector<SeqKafkaPkg *> pkgs;
  // try lock, not block producer add wait_update_pkgs_
  int ret = pthread_mutex_trylock(&wait_update_pkgs_mutex_);
  switch (ret) {
    case 0:
      // get all SeqKafkaPkgs
      while (!wait_update_pkgs_.empty()) {
        SeqKafkaPkg *pkg = wait_update_pkgs_.front();
        pkgs.push_back(pkg);
        wait_update_pkgs_.pop();
      }

      PthreadCall("pthread_mutex_unlock",
                  pthread_mutex_unlock(&wait_update_pkgs_mutex_));
      break;

    case EINVAL:
    case EAGAIN:
    case EDEADLK:
    case EPERM:
    case EBUSY:
    default:
      PthreadCall("pthread_mutex_unlock",
                  pthread_mutex_unlock(&wait_update_pkgs_mutex_));
      return;
  }

  for (size_t i = 0; i < pkgs.size(); ++i) {
    SeqKafkaPkg *seq_pkg = pkgs[i];
    string seq_pkg_key = seq_pkg->Key();
    uint64_t trans_seq_no = seq_pkg->trans->trans_seq_no;

    RDP_LOG_DBG << "CheckPoint Proc Gtid:" << seq_pkg->trans->gtid
               << ", Key:" << seq_pkg_key << " Offset:" << seq_pkg->offset;

    SetEpoch(seq_pkg->trans->epoch);

    switch (seq_pkg->split_flag) {
      // if not split pkg update checkpoint gtidset
      case PKG_NOT_SPLIT:
        // if transaction has gtid, update the checkpoint gtidset
        // because transaction may be did not have gtid
        if (!seq_pkg->trans->gtid.empty()) {
          // if update checkpoint failt, did not have to exit program,
          // if program reboot checkpoint rebuild gtidset will fix it.
          if (!AddGtid(seq_pkg->trans->gtid)) {
            RDP_LOG_ERROR << "Add Gtid: " << seq_pkg->trans->gtid << " Failt";
          }
        }

        // write a batch kafka message only return one offset
        // but every message has it own offset in kafka
        // first kafka message offset = 0
        // seq_pkg->offset is uint64_t
        //if (seq_pkg->offset >= 0) {

          RDP_LOG_DBG << "Checkpoint Update Offset: " << seq_pkg->offset
                     << ", Transaction SeqNo: " << seq_pkg_key;

          // update transaction must after update offset
          SetTransSeqNo(trans_seq_no);

          // if update checkpoint failt, did not have to exit program,
          // if program reboot checkpoint rebuild gtidset will fix it.
          UpdateOffset(seq_pkg->offset);
        //}

#ifdef STAT_TIME
        STAT_TIME_POINT(STAT_T13, seq_pkg->trans->trans_seq_no,
                        seq_pkg->trans->stat_point);
#endif

#ifndef NO_KAFKA
        STAT_TIME_POINT_END(seq_pkg->trans->stat_point);
#endif

        // total wrote kafka transaction metric
        g_syncer_metric->total_done_trx_count_metric_->AddValue(1);

        // free SeqTrans
        delete seq_pkg->trans;
        seq_pkg->trans = NULL;

        break;

      // if split pkg, but not the end pkg, not update checkpoint gtidset
      // wait for the end split pkg
      case PKG_SPLIT:
        break;

      // if split pkg and the end pkg, update checkpoint gtidset
      case PKG_SPLIT_END:
        if (!seq_pkg->trans->gtid.empty()) {
          if (!AddGtid(seq_pkg->trans->gtid)) {
            RDP_LOG_ERROR << "Checkpoint Add Gtid: " << seq_pkg->trans->gtid
                       << ", To Gtid Mgr Failt";
          }
        }

        // write a batch kafka message only return one offset
        // but every message has it own offset in kafka
        // first kafka message offset = 0
        // seq_pkg->offset is uint64_t
        //if (seq_pkg->offset >= 0) {

          RDP_LOG_DBG << "Checkpoint Update Offset: " << seq_pkg->offset
                     << ", Transaction SeqNo: " << seq_pkg_key;

          // writed to kafka, then update checkpoint offset

          // if update checkpoint failt, did not have to exit program,
          // if program reboot checkpoint rebuild gtidset will fix it.
          UpdateOffset(seq_pkg->offset);
          // update transaction must after update offset
          SetTransSeqNo(trans_seq_no);
        //}

#ifdef STAT_TIME
        STAT_TIME_POINT(STAT_T13, seq_pkg->trans->trans_seq_no,
                        seq_pkg->trans->stat_point);
#endif

#ifndef NO_KAFKA
        STAT_TIME_POINT_END(seq_pkg->trans->stat_point);
#endif

        // total wrote kafka transaction metric
        g_syncer_metric->total_done_trx_count_metric_->AddValue(1);

        // free SeqTrans
        delete seq_pkg->trans;
        seq_pkg->trans = NULL;

        break;

      default:
        abort();
    }

    // calculate recv write kafka response avg rt
    g_syncer_metric->write_msg_avg_metric_->PushValue(
        rdp_comm::GetIntervalMs(&seq_pkg->sent, &seq_pkg->recv_rsp));

    // total wrote kafka message metric
    g_syncer_metric->total_done_msg_count_metric_->AddValue(1);

    // free SeqKafkaPkg
    delete seq_pkg;
  }

  pkgs.clear();
}

}  // namespace syncer
