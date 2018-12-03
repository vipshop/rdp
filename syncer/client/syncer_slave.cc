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

#include "syncer_slave.h"
#include "binlog_writer.h"
#include "bounded_buffer.h"
#include "syncer_def.h"
#include "trx_boundary_parser.h"
#include <rdp-comm/leader_election.h>
#include <rdp-comm/util.h>
#include <rdp-comm/zk_config.h>
#include <rdp-comm/zk_process.h>
#include <rdp-comm/fiu_engine.h>
#include "memory_pool.h"
#include "rdp_syncer_metric.h"

using namespace rdp_comm;
using namespace fiu;

namespace syncer {

const unsigned char kPacketMagicNum = 0xef;
const unsigned char kPacketFlagSync = 0x01;

static char cur_logname[FN_REFLEN + 1];
static my_off_t old_off;
static char log_file_name[FN_REFLEN + 1];
static char *log_file = NULL;

// Reply buffer to fill binlog position and file name
static std::string master_binlog_basename = "bin";
static char semi_sync_reply_buffer[64] = {0x0};
static uint32_t reply_payload_size = 0;
static uint32_t binlog_filename_size = 0;

enum_binlog_checksum_alg g_checksum_alg = binary_log::BINLOG_CHECKSUM_ALG_OFF;
BinlogWriter *binlog_writer = nullptr;

//Transaction_boundary_parser trx_boundary_parser;
static bool retry_connect = true;
static bool new_master_purged = false;
static long clock_diff_with_master = 0;
// The sequence number for each trx
static uint64_t g_trx_seqno = 1;   

// How many times we try to connect to MySQL master,
// For the 1st time, we take gtidset from checkpoint in Zookeeper;
// Or else, we take gtidset from syncer_progress that identify
// all transactions had pushed to bounded_buffer
static uint64_t retry_counter = 0;

static TrxMsg *g_trx_msg = NULL;

struct MergePurgedGtid {
  Sid_map *map;
  Gtid_set *set;
  MYSQL *mysql;

  explicit MergePurgedGtid(MYSQL *m)
      : map(new Sid_map(NULL)), set(new Gtid_set(map)), mysql(m) {}
  ~MergePurgedGtid(void) {
    delete set;
    set = NULL;
    delete map;
    map = NULL;
  }

  // Merge current mysql(new master) instance's purged gtidset to
  // managed global gtidset within SyncedProgress
  bool Merge(void) {
    MYSQL_RES *res = NULL;
    MYSQL_ROW row;
    SyncedProgress *progress = g_syncer_app->synced_progress_;
    assert(progress);

    const char *query_str = "show global variables like 'gtid_purged'";
    if (!mysql_real_query(mysql, query_str, strlen(query_str)) &&
        (res = mysql_store_result(mysql)) && (row = mysql_fetch_row(res))) {
      RDP_LOG_DBG << "New Master's gtid_purged: " <<  row[1];
      if (RETURN_STATUS_OK != set->add_gtid_text(row[1])) {
        RDP_LOG_ERROR << "MergePurgedGtid Merge gtid_purged Gtid: " << row[1]
                   << ", Error";
        if (res) {
          mysql_free_result(res);
          res = nullptr;
          return false;
        }
      }
      if (!progress->Merge(set)) {
        RDP_LOG_ERROR << "MergePurgedGtid Process Merge set Error";
        if (res) {
          mysql_free_result(res);
          res = nullptr;
          return false;
        }
      }

      if (res) {
        mysql_free_result(res);
        res = nullptr;
      }
      return true;
    } else {
      RDP_LOG_ERROR << "Execution failed on master: " << query_str;
      return false;
    }
  }
};

DumpPreparation::DumpPreparation(MYSQL *mysql) : mysql_(mysql) {}
DumpPreparation::~DumpPreparation() {}

// Do all necessary preparation action before start dump
// binlog from MySQL master server
bool DumpPreparation::DoPrepare() {
  if (!VerifyMasterVersion()) return false;

  if (!VerifyFencingToken()) return false;

  if (!QueryMasterTimestamp()) return false;

  if (!QueryMasterServerId()) return false;

  if (!QueryMasterUUID()) return false;

  if (!QueryMasterBinlogBasename()) return false;

  if (!SetHeartbeatPeriod()) return false;

  if (!SetRDPSessionUUID()) return false;

  if (!RegisterToMaster()) return false;

  if (!StartupSemiSyncDump()) return false;

  if (!SetBinlogChecksum()) return false;

  return true;
}

// rdp only support the specify version of mysql master now.
// This function need to be changed if support muti-version
bool DumpPreparation::VerifyMasterVersion(void) {
  AppConfig *conf = g_syncer_app->app_conf_;
  MYSQL_RES *res = NULL;
  MYSQL_ROW row;
  const char *query = NULL;
  std::string config_version;
  std::string master_version;

  config_version = conf->GetValue<std::string>("mysql.version");
  if (config_version.empty()) {
    RDP_LOG_ERROR << "mysql.version must be set in config file.";
    goto err_t;
  }

  // Query version of current master
  query = "select version()";
  if (!mysql_real_query(mysql_, query, strlen(query)) &&
      (res = mysql_store_result(mysql_)) && (row = mysql_fetch_row(res))) {
    master_version = row[0];
    RDP_LOG_INFO << "Current master version is: " << master_version << ".";
    if (res) {
      mysql_free_result(res);
      res = NULL;
    }
  } else {
    RDP_LOG_ERROR << "Execution failed on master: (" << query << ").";
    goto err_t;
  }

  if (config_version != master_version) {
    RDP_LOG_ERROR << "Master version (" << master_version
               << ") is not supported, the supported version is ("
               << config_version << ").";
    goto err_t;
  }

  RDP_LOG_INFO << "Master version checked.";
  return true;

err_t:
  return false;
}

// If fencing token mode is enabled, try to check if a valid fencing
// token, indicates we are the real rdp leader and should reconnect in case
// of failure
bool DumpPreparation::VerifyFencingToken(void) {
  AppConfig *conf = g_syncer_app->app_conf_;
  MYSQL_RES *res = NULL;
  MYSQL_ROW row;
  const char *query = NULL;
  char query_buf[256];

  int fencing_token = conf->GetValue<int>("fencing.token.enable", 0);
  if (fencing_token) {
    // Query current fencing token
    unsigned long current_token = 0;
    query = "select @@rdp_ha_fencing_token";
    if (!mysql_real_query(mysql_, query, strlen(query)) &&
        (res = mysql_store_result(mysql_)) && (row = mysql_fetch_row(res))) {
      current_token = strtoul(row[0], 0, 10);
      RDP_LOG_DBG << "Current rdp fencing token is: " << current_token;
      if (res) {
        mysql_free_result(res);
        res = NULL;
      }
    } else {
      RDP_LOG_ERROR << "Execution failed on master: " << query;
      goto err_t;
    }

    // Try to upgrade fencing token
    rdp_comm::LeaderElection *leader =
        &rdp_comm::Singleton<rdp_comm::LeaderElection>::GetInstance();
    snprintf(query_buf, sizeof(query_buf),
             "set global rdp_ha_fencing_token=%lu", leader->GetVersion());
    RDP_LOG_DBG << "query string: " << query_buf;
    if (mysql_query(mysql_, query_buf)) {
      retry_connect = false;
      RDP_LOG_ERROR <<
          "Try to verify rdp token failed, "
          "Master returned " << mysql_error(mysql_);
      goto err_t;
    }
    mysql_free_result(mysql_store_result(mysql_));
    retry_connect = true;
    RDP_LOG_DBG << "RDP fencing token upgraded from " << current_token << " to " << leader->GetVersion();
  }
  return true;

err_t:
  g_syncer_metric->token_illegal_count_metric_->AddValue(1);
  return false;
}

// After connect to MySQL Master, we already got the version
// of remote master, version is retrieved during authorizing
// #1 Differentiate timestamp gap between master and rdp
bool DumpPreparation::QueryMasterTimestamp(void) {
  MYSQL_RES *res = NULL;
  MYSQL_ROW row;
  const char *query = NULL;

  query = "SELECT UNIX_TIMESTAMP()";
  if (!mysql_real_query(mysql_, query, strlen(query)) &&
      (res = mysql_store_result(mysql_)) && (row = mysql_fetch_row(res))) {
    unsigned long master_ts = strtoul(row[0], 0, 10);
    clock_diff_with_master = (unsigned long)time(0) - master_ts;
    RDP_LOG_INFO << "Time delta with master: " << clock_diff_with_master << "s";
    if (res) {
      mysql_free_result(res);
      res = NULL;
    }
  } else {
    RDP_LOG_ERROR << "Execution failed on master: " << query;
    goto err_t;
  }

  return true;

err_t:
  return false;
}

// #2 Download master server id
bool DumpPreparation::QueryMasterServerId(void) {
  MYSQL_RES *res = NULL;
  MYSQL_ROW row;
  const char *query = NULL;

  query = "SELECT @@GLOBAL.SERVER_ID";
  if (!mysql_real_query(mysql_, query, strlen(query)) &&
      (res = mysql_store_result(mysql_)) && (row = mysql_fetch_row(res))) {
    unsigned long master_id = strtoul(row[0], 0, 10);
    RDP_LOG_DBG << "Master server id is " <<  master_id;
    if (res) {
      mysql_free_result(res);
      res = nullptr;
    }
  } else {
    RDP_LOG_ERROR << "Execution failed on master: "<<  query;
    goto err_t;
  }

  return true;

err_t:
  return false;
}

// #3: heartbeat current ignored
// #4: Download master uuid, issue warning about which transactions probably
// loss.
// To avoid we encounter very long gtidset string, construct query command via
// std::string
bool DumpPreparation::QueryMasterUUID(void) {
  SyncedProgress *progress = g_syncer_app->synced_progress_;
  MYSQL_RES *res = NULL;
  MYSQL_ROW row;
  const char *query = NULL;
  bool master_switched = false;

  query = "SELECT @@GLOBAL.SERVER_UUID";
  if (!mysql_real_query(mysql_, query, strlen(query)) &&
      (res = mysql_store_result(mysql_)) && (row = mysql_fetch_row(res))) {
    RDP_LOG_DBG << "Master server's UUID: " << row[0];

    FIU_EXECUTE_IF("simulate_master_switched",
    { progress->SetMasterUuid("b3927e40-mmmm-22e6-83a8-90b11c3c9447"); });

    const char *master_uuid_str = progress->GetMasterUuid();
    if (0x0 != master_uuid_str[0] && memcmp(row[0], master_uuid_str, 36)) {
      RDP_LOG_DBG << "Probably MySQL master switched from " << progress->GetMasterUuid() << " to " << row[0];
      // TODO: alter !!! diff two gtidsets, check if we had lost some
      // transactions here?
      master_switched = true;
      g_syncer_metric->mysql_switch_count_metric_->AddValue(1);
    }

    progress->SetMasterUuid(row[0]);
    if (res) {
      mysql_free_result(res);
      res = nullptr;
    }

    // Detect master switched, we do more verify such that we can know
    // more detail about whether master/slave is not synced timely
    if (master_switched) {
      // Retrieve new master's gtid_executed, change from 'show variables'
      // to 'select @@global.' such that skip the 1024 bytes limitation
      std::string new_gtid_executed = "";
      query = "select @@global.gtid_executed;";
      if (!mysql_real_query(mysql_, query, strlen(query)) &&
          (res = mysql_store_result(mysql_)) && (row = mysql_fetch_row(res))) {
        RDP_LOG_DBG << "New Master's gtid_executed is: " << row[0];
        new_gtid_executed = std::string(row[0]);

        if (res) {
          mysql_free_result(res);
          res = nullptr;
        }
      } else {
        RDP_LOG_ERROR << "Execution failed on master: " << query;
        goto err_t;
      }

      // Rdp owned gtidset
      std::string rdp_gtid_owned = progress->GetGTIDSet();

      // Diff two gtidset to verify if there is data loss
      std::string cmd_str = "select gtid_subtract('" + rdp_gtid_owned + "', '" +
                            new_gtid_executed + "')";
      RDP_LOG_DBG << "query string: " << cmd_str.c_str();
      if (!mysql_real_query(mysql_, cmd_str.c_str(), cmd_str.size()) &&
          (res = mysql_store_result(mysql_)) && (row = mysql_fetch_row(res))) {
        std::string lost_trans = std::string(row[0]);
        if (!lost_trans.empty()) {
          RDP_LOG_ERROR << "Probably are lost: " << lost_trans.c_str();
        } else {
          RDP_LOG_WARN << "MySQL switched with no data los.";
        }

        if (res) {
          mysql_free_result(res);
          res = nullptr;
        }
      } else {
        RDP_LOG_ERROR << "Execution failed on master: " << cmd_str.c_str();
        goto err_t;
      }
    }
  } else {
    RDP_LOG_ERROR << "Execution failed on master: " << query;
    goto err_t;
  }

  return true;

err_t:
  return false;
}

// #5: Download master binlog basename
bool DumpPreparation::QueryMasterBinlogBasename(void) {
  MYSQL_RES *res = NULL;
  MYSQL_ROW row;
  const char *query = NULL;

  query = "SELECT @@GLOBAL.log_bin_basename";
  if (!mysql_real_query(mysql_, query, strlen(query)) &&
      (res = mysql_store_result(mysql_)) && (row = mysql_fetch_row(res))) {
    master_binlog_basename = std::string(basename(row[0]));
    RDP_LOG_DBG << "Master server's binlog basename: " << master_binlog_basename.c_str();

    binlog_filename_size = master_binlog_basename.size() + 1 + 6;
    reply_payload_size = binlog_filename_size + 1 + 8;
    assert(reply_payload_size + 4 < sizeof(semi_sync_reply_buffer));
    int3store(semi_sync_reply_buffer, reply_payload_size);
    semi_sync_reply_buffer[3] = 0;
    semi_sync_reply_buffer[4] = kPacketMagicNum;

    if (res) {
      mysql_free_result(res);
      res = nullptr;
    }
  } else {
    RDP_LOG_ERROR << "Execution failed on master: " << query;
    goto err_t;
  }

  return true;

err_t:
  return false;
}

// #6: Set heartbeat period
bool DumpPreparation::SetHeartbeatPeriod(void) {
  AppConfig *conf = g_syncer_app->app_conf_;
  // Default heartbeat period is 600s
  int heartbeat_period = conf->GetValue<int>("syncer.heartbeat.period", 600);
  char query_buf[256];
  int query_len = 0;
 
  // The master_heartbeat_period is an ulonglong of nano-secs.  
  query_len = snprintf(query_buf, sizeof(query_buf), 
      "SET @master_heartbeat_period= %lld", (ulonglong)heartbeat_period*1000000000UL);
  if (mysql_real_query(mysql_, query_buf, query_len)) {
    RDP_LOG_ERROR << "Execution failed on master: " <<  query_buf;
    goto err_t;
  }

  return true;

err_t:
  return false;
}

// #7: Set rdp uuid to current session
bool DumpPreparation::SetRDPSessionUUID(void) {
  AppConfig *conf = g_syncer_app->app_conf_;
  char query_buf[256];
  int query_len = 0;

  query_len = snprintf(query_buf, sizeof(query_buf), "SET @slave_uuid= '%s'",
                       conf->GetValue<std::string>("syncer.uuid").c_str());
  if (mysql_real_query(mysql_, query_buf, query_len)) {
    RDP_LOG_ERROR << "Execution failed on master: " <<  query_buf;
    goto err_t;
  }

  return true;

err_t:
  return false;
}

// #8: Register on master
bool DumpPreparation::RegisterToMaster(void) {
  AppConfig *conf = g_syncer_app->app_conf_;
  uchar buf[1024], *pos = buf;
  int4store(pos, conf->GetValue<int>("server.id"));
  pos += 4;
  *pos++ = 0;
  *pos++ = 0;
  *pos++ = 0;
  int2store(pos, (uint16)conf->GetValue<int>("mysql.port"));
  pos += 2;

  // Fake rpl_recovery_rank, which was removed in BUG#13963,
  // so that this server can register itself on old servers,
  // see BUG#49259.
  int4store(pos, /* rpl_recovery_rank */ 0);
  pos += 4;
  /* The master will fill in master_id */
  int4store(pos, 0);
  pos += 4;

  if (simple_command(mysql_, COM_REGISTER_SLAVE, buf, (size_t)(pos - buf), 0)) {
    RDP_LOG_ERROR << "COM_REGISTER_SLAVE: " << mysql_error(mysql_);
    goto err_t;
  }

  return true;

err_t:
  return false;
}

// #9: Check if master server has semi-sync plugin installed
bool DumpPreparation::StartupSemiSyncDump(void) {
  AppConfig *conf = g_syncer_app->app_conf_;
  MYSQL_RES *res = NULL;
  MYSQL_ROW row;
  const char *query = NULL;

  if (conf->GetValue<int>("semi.sync", 0)) {
    // #8: Check if master server has semi-sync plugin installed
    query = "SHOW VARIABLES LIKE 'rpl_semi_sync_master_enabled'";
    if (mysql_real_query(mysql_, query, strlen(query)) ||
        !(res = mysql_store_result(mysql_))) {
      RDP_LOG_ERROR << "Execution failed on master: " << query;
      goto err_t;
    }

    row = mysql_fetch_row(res);
    if (!row) {
      /* Master does not support semi-sync */
      RDP_LOG_ERROR <<
          "Master server does not support semi-sync, "
          "fallback to asynchronous replication";
      goto err_t;
    }

    if (res) {
      mysql_free_result(res);
      res = nullptr;
    }

    // #9: Tell master dump thread that we want to do semi-sync replication
    query = "SET @rpl_semi_sync_slave= 1";
    if (mysql_real_query(mysql_, query, strlen(query))) {
      RDP_LOG_ERROR << "Set 'rpl_semi_sync_slave=1' on master failed.";
      goto err_t;
    }
    mysql_free_result(mysql_store_result(mysql_));
  }

  return true;

err_t:
  return false;
}

// #10: Make a notice to the server that this client is checksum-aware.
bool DumpPreparation::SetBinlogChecksum(void) {
  if (mysql_query(mysql_, "SET @master_binlog_checksum='NONE'")) {
    RDP_LOG_ERROR <<
        "Could not notify master about checksum awareness."
        "Master returned " << mysql_error(mysql_);
    goto err_t;
  }
  mysql_free_result(mysql_store_result(mysql_));
  return true;

err_t:
  return false;
}

// Verify semi-sync packet and extract info
static inline int SlaveReadSemiSyncHeader(const char *header,
                                          unsigned long total_len,
                                          bool *need_reply,
                                          const char **payload,
                                          unsigned long *payload_len) {
  int ret = 0;
  if ((unsigned char)(header[0]) == kPacketMagicNum) {
    *need_reply = (header[1] & kPacketFlagSync);
    *payload_len = total_len - 2;
    *payload = header + 2;
  } else {
    RDP_LOG_ERROR << "Missing magic number for semi-sync packet, packet len: " << total_len;
    ret = -1;
  }

  return ret;
}

// A slave replies to the master indicating its replication process.  It
// indicates that the slave has received all events before the specified
// binlog position.
//
// Input:
//  mysql            - (IN)  the mysql network connection
//  binlog_filename  - (IN)  the reply point's binlog file name
//  binlog_filepos   - (IN)  the reply point's binlog file offset
//
// Return:
//  0: success;  non-zero: error
static inline int ReplySemiSyncAckToMaster(NET *net,
                                           const char *binlog_filename,
                                           my_off_t binlog_filepos) {
  int8store(semi_sync_reply_buffer + 5, binlog_filepos);
  memcpy(semi_sync_reply_buffer + 13, binlog_filename,
         binlog_filename_size + 1 /* including trailing '\0' */);
  return SyncSend(net->fd, (const uint8_t *)semi_sync_reply_buffer,
                  reply_payload_size + 4)
             ? 0
             : 1;
}

// Write a log event to file and reply if master required!
static bool WriteBinlogAndReply(NET *net, const char *event_buf,
                                uint32_t event_len, const char *log_file,
                                bool need_reply) {
  EventMsg *em = NULL;
  size_t msg_size = 0;
  char* ptr = NULL;
  bool encounter_trx_boundary = false;                                                                                                                    
  bool is_ddl = false;

  SyncedProgress *progress = g_syncer_app->synced_progress_;

  PreHeader *ph = (PreHeader *)event_buf;
  if (ph->event_type <= binary_log::UNKNOWN_EVENT ||
      ph->event_type >= binary_log::ENUM_END_EVENT) {
    RDP_LOG_ERROR << "Reply: Illegal event type " << ph->event_type 
      << " @ " << log_file << ":" << ph->log_pos;
    goto err_t;
  }

  // ACK before write file
  if (/*app_cnf_->ack_before_write_ && */ need_reply) {
    if (ReplySemiSyncAckToMaster(net, log_file, ph->log_pos)) {
      goto err_t;
    }
  }

  // Cache latest Gtid
  if (ph->event_type == binary_log::GTID_LOG_EVENT) {
    progress->CacheLatestGtid(event_buf, ph->event_size);
  }

  if (NULL == g_trx_msg) {
    g_trx_msg = new TrxMsg(g_trx_seqno);
    STAT_TIME_POINT(STAT_T1, g_trx_msg->seq_no, g_trx_msg->stat_point);
  }

  // If feed_event detect invalid case, log errors
  if (g_trx_boundary_parser.feed_event(event_buf, event_len, true)) {
    RDP_LOG_ERROR << "Trx boundary parser feed_event!";
    goto err_t;
  }
  encounter_trx_boundary = g_trx_boundary_parser.is_not_inside_transaction();
  is_ddl = g_trx_boundary_parser.is_ddl();


  msg_size = sizeof(EventMsg) + 32 + ph->event_size + 1;
  // try 100 times, every time is 20 milisecondd
  for (int32_t i = 0; i < 100; i++) {
    ptr = (char*)g_memory_pool->malloc(msg_size);
    if (ptr) break;
    usleep(20 * 1000);
  }
  if (NULL == ptr) {
    g_syncer_app->Alarm("Alloc memory from memory pool failed");
    exit(EXIT_FAILURE);
  }
  em = (EventMsg*)ptr;

  em->len = ph->event_size;
  ptr += sizeof(EventMsg);
  
  em->binlog_fname = ptr;
  memcpy(em->binlog_fname, log_file, binlog_filename_size);
  em->binlog_fname[binlog_filename_size] = 0x0;
  ptr += 32;

  em->msg = ptr;
  memcpy(em->msg, event_buf, ph->event_size);
  em->msg[ph->event_size] = 0x0;
  g_trx_msg->AddEvent(em, is_ddl ? TRX_DDL : TRX_DML);

  if (encounter_trx_boundary) {
    g_trx_msg->gtid = progress->GetLatestGtid();
    g_trx_msg->checksum = g_checksum_alg;

    FIU_EXECUTE_IF("reconnect_in_uncompleteed_trx", {
        goto err_t;
    });

    // A full transaction had being delivered, update gtidset_executed
    if (!progress->Commit()) {
      RDP_LOG_ERROR << "SyncedProgress Commit failt";
      goto err_t;
    }

    g_trx_boundary_parser.reset();
    STAT_TIME_POINT(STAT_T2, g_trx_msg->seq_no, g_trx_msg->stat_point);
    g_trx_bounded_buffer->Push(g_trx_msg);
    g_trx_seqno++;
    g_trx_msg = NULL;
    STAT_TIME_POINT(STAT_T3, g_trx_msg->seq_no, g_trx_msg->stat_point);

  }

  g_syncer_metric->total_msg_count_metric_->AddValue(1);

  return true;

err_t:
  return false;
}

// Print log event to log file, sometimes we ignore log events,
// for debug purpose, keep a trace of the event we have ignored
inline void PrintLogEvent(const PreHeader *ph, bool need_reply,
                          bool ignored = false) {
  char buf[256];
  snprintf(buf, sizeof(buf),
      "[%s]: time(%010u) - type(%03u) - server_id(%u) - size(%u) - "
      "log_pos(%u) - flag(%hu) - reply(%s)",
      ignored ? "Ignored" : "Normal" , ph->timestamp, ph->event_type,
      ph->server_id, ph->event_size, ph->log_pos, ph->flag,
      need_reply ? "Y" : "N");
  RDP_LOG_DBG << buf;
}

MySQLSlave::MySQLSlave() 
  : mysql_(NULL), 
    rate_limiter_(NULL) { 
  DBUG_INIT(); 
}

MySQLSlave::~MySQLSlave() {
  if (rate_limiter_) {
    delete rate_limiter_;
    rate_limiter_ = NULL;
  }
  this->SlaveClearup();
  DBUG_CLOSE();
}

// Connect to source MySQL server
bool MySQLSlave::SlaveStartup(void) {
  AppConfig *conf = g_syncer_app->app_conf_;
  char query_buf[256];
  int query_len = 0;
  enum mysql_ssl_mode ssl_mode= SSL_MODE_DISABLED;

  // Setup mysql connection
  this->SlaveClearup();
  mysql_ = mysql_init(NULL);
  if (mysql_ == NULL) {
    RDP_LOG_ERROR << "mysql_init: " <<  mysql_error(mysql_);
    return false;
  }
  
  if (mysql_options(mysql_, MYSQL_OPT_SSL_MODE, &ssl_mode)) {
    RDP_LOG_ERROR << "mysql_options: " <<  mysql_error(mysql_);
    this->SlaveClearup();
    return false;
  }

  if (mysql_real_connect(
          mysql_, conf->GetValue<std::string>("mysql.vip").c_str(),
          conf->GetValue<std::string>("mysql.user").c_str(),
          conf->GetValue<std::string>("mysql.password").c_str(), NULL,
          conf->GetValue<int>("mysql.port"), NULL, 0) == NULL) {
    RDP_LOG_ERROR << "mysql_real_connect: " << mysql_error(mysql_);
    this->SlaveClearup();
    return false;
  }

  // Set the net_write_timeout
  int net_write_timeout = conf->GetValue<int>("syncer.net_write_timeout", 600);
  query_len = snprintf(query_buf, sizeof(query_buf), 
      "SET net_write_timeout= %d", net_write_timeout);
  if (mysql_real_query(mysql_, query_buf, query_len)) {
    RDP_LOG_ERROR << "Execution failed on master: " <<  query_buf << ", " << mysql_error(mysql_);
    this->SlaveClearup();
    return false;
  }


  RDP_LOG_INFO << "Connect to MySQL Master Ok, " << 
          conf->GetValue<std::string>("mysql.vip").c_str() << ":" <<
          conf->GetValue<int>("mysql.port");
  
  memset(cur_logname, 0, sizeof(cur_logname));
  memset(log_file_name, 0, sizeof(log_file_name));
  g_trx_boundary_parser.reset();
  g_syncer_app->synced_progress_->ResetLatestGtid();
  if (g_trx_msg) {
    delete g_trx_msg;
    g_trx_msg = NULL;
  }

  g_syncer_metric->connect_master_count_metric_->AddValue(1);
  return true;
}

// Do preparation before real dump request
bool MySQLSlave::SlavePrepare(void) {
  DumpPreparation dp(mysql_);
  return dp.DoPrepare();
}

bool MySQLSlave::SlaveRequest(void) {
  bool ret = true;
  char *ptr = nullptr, *base = nullptr;

  // Initial GTID set
  uint32_t blen = 0;
  uint8_t exist_gtidset[4096] = {0x0};
  SyncedProgress *progress = g_syncer_app->synced_progress_;
  AppConfig *conf = g_syncer_app->app_conf_;
  CheckPoint *checkpoint = &rdp_comm::Singleton<CheckPoint>::GetInstance();


  if (0 == retry_counter) {
    if (!progress->SetGTIDSet(
             (const uint8_t *)checkpoint->GetGtidSet().c_str())) {
      RDP_LOG_ERROR << "Slave Process Set GTID Set: " << checkpoint->GetGtidSet()
                 << ", From CheckPoint Failt";
      return false;
    }
    RDP_LOG_INFO << "Initiate gtidset from ZK checkpoint";
  } else {
    // If last dump request failed due to new master purged some gtids generate
    // @ new master (Not replicate from old master). To fix the dump connection,
    // we merge purged gtids to rdp's exist gtidset, such that the dump
    // connection can be established normally
    if (new_master_purged) {
      RDP_LOG_WARN << "Merge master's purged gtid";
      new_master_purged = false;
      MergePurgedGtid mpg(mysql_);
      FIU_EXECUTE_IF("simulate_merge_purged_failed", {
        return false;
      });
      mpg.Merge();
      //if (!FIU_EVALUATE_IF("simulate_merge_purged_failed", false, mpg.Merge()))
      //  return false;
    }
  }

  RDP_LOG_INFO << "Checkpoint gtid: " <<  checkpoint->GetGtidSet().c_str();
  progress->Dump();

  blen = progress->GetGTIDSet(exist_gtidset);
  // Calculate the desired size of packet, in case of
  // insufficient allocate dynamically; If switch over too frequently
  // between different server, the UUID string can be expanded ...
  // MySQL 5.7.17 rpl_slave.cc::request_dump(...)
  const size_t desired_size =
      ::BINLOG_FLAGS_INFO_SIZE + ::BINLOG_SERVER_ID_INFO_SIZE +
      ::BINLOG_NAME_SIZE_INFO_SIZE + binlog_filename_size +
      ::BINLOG_POS_INFO_SIZE + ::BINLOG_DATA_SIZE_INFO_SIZE + blen;

  base = new char[desired_size + 1];
  ptr = base;

  // 2 Bytes flag
  int2store(ptr, 0);
  ptr += ::BINLOG_FLAGS_INFO_SIZE;

  // 4 bytes server id
  int4store(ptr, conf->GetValue<int>("server.id", 10000));
  ptr += ::BINLOG_SERVER_ID_INFO_SIZE;

  // 4 bytes log file name length
  int4store(ptr, binlog_filename_size);
  ptr += ::BINLOG_NAME_SIZE_INFO_SIZE;

  // N bytes if log file name string
  memset(ptr, 0x0, binlog_filename_size);
  ptr += binlog_filename_size;

  // 8 bytes offset
  int8store(ptr, 4LL);
  ptr += ::BINLOG_POS_INFO_SIZE;

  // 4 bytes of gtid string length
  int4store(ptr, static_cast<uint32>(blen));
  ptr += ::BINLOG_DATA_SIZE_INFO_SIZE;

  // N bytes of gtid string
  memcpy(ptr, exist_gtidset, blen);
  ptr += blen;

  if (simple_command(mysql_, COM_BINLOG_DUMP_GTID, (const uchar *)base,
                     ptr - base, 1)) {
    RDP_LOG_ERROR << "Got fatal error sending the log dump command";
    ret = false;
  }

  delete[] base;
  base = nullptr;
  return ret;
}

// Busy dump loop
static const char *master_purged_error =
    "The slave is connecting using CHANGE MASTER TO MASTER_AUTO_POSITION = 1, "
    "but the master has purged binary logs containing GTIDs that the slave "
    "requires.";

// Read log event from remote MySQL master, group in transaction granularity
// and push to bounded queue, coordinator thread pick from bounded queue
bool MySQLSlave::SlaveProcess(void) {
  ulong len = 0;
  bool semi_sync_need_reply = false;
  AppConfig *conf = g_syncer_app->app_conf_;
  int semisync_enable = conf->GetValue<int>("semi.sync", 0);
  int sync_method = conf->GetValue<int>("sync.method", 0);

  Log_event_type type = binary_log::UNKNOWN_EVENT;
  NET *net = &(mysql_->net);

  // Expose the io thread state, 1 means connected
  g_syncer_metric->io_thread_state_metric_->SetValue(1);

  if (binlog_writer) {
    binlog_writer->Close();
    delete binlog_writer;
    binlog_writer = nullptr;
  }
  binlog_writer = new PosixWriter((FileSyncMethod)sync_method);

  while (signalhandler::IsRunning()) {
    len = cli_safe_read(mysql_, NULL);
    FIU_EXECUTE_IF("simulate_dump_error", {
      len = 0;
      mysql_->net.last_errno = ER_MASTER_HAS_PURGED_REQUIRED_GTIDS;
    });

    if (len == packet_error || (long)len < 1) {
      RDP_LOG_ERROR << "'cli_safe_read' failed: (" 
        << mysql_errno(mysql_) 
        << " - " << ER_MASTER_HAS_PURGED_REQUIRED_GTIDS 
        << " - " << mysql_error(mysql_) << ").";
      // Next time when retry connect, we merge new master's purged gtidset
      // togather, such that we can establish dump connection to new master
      // successfully
      // 对比错误码的逻辑不对 要根据error string来判断 对应issue:#120
      //if (ER_MASTER_HAS_PURGED_REQUIRED_GTIDS == mysql_errno(mysql_)) {
      //  RDP_LOG_DBG << "Will try to merge purged gtidset next round!";
      //  new_master_purged = true;
      //}
      if (0 == strcasecmp(master_purged_error, mysql_error(mysql_))) {
        RDP_LOG_DBG << "Will try to merge purged gtidset next round!";
        new_master_purged = true;
      }
      goto err_t;
    }

    // End of data
    if (len < 8 && net->read_pos[0] == 254) {
      break;
    }

    // Limit the net io rate
    rate_limiter_->Wait(len);
    g_syncer_metric->in_rate_metric_->AddValue(len);

    const char *event_buf = (const char *)net->read_pos + 1;
    ulong event_len = len - 1;

    // Verify and extract info
    if (semisync_enable) { // The position of event type is diffent when semisync 
      type = (Log_event_type)net->read_pos[1 + 2 + EVENT_TYPE_OFFSET];
      if (SlaveReadSemiSyncHeader((const char *)net->read_pos + 1, event_len,
                                  &semi_sync_need_reply, &event_buf,
                                  &event_len)) {
        RDP_LOG_ERROR << "Malformed semi-sync packet" ;
        abort();
      }
    } else {
      type = (Log_event_type)net->read_pos[1 + EVENT_TYPE_OFFSET];
    }

    time_t now = time(0);
    PreHeader *ph = (PreHeader *)event_buf;

    g_syncer_metric->binlog_received_count_metric_->AddValue(1);
    g_syncer_metric->last_event_timestamp_metric_->SetValue(now);

    if (ph->timestamp != 0) {
      // Expose seconds_behind_master
      long seconds_behind_master = now - ph->timestamp - clock_diff_with_master;
      g_syncer_metric->seconds_behind_master_metric_->SetValue(seconds_behind_master>0 ? seconds_behind_master : 0);
    }

    // Ignore heartbeat
    if (type == binary_log::HEARTBEAT_LOG_EVENT) {
      g_syncer_metric->binlog_heartbeat_received_count_metric_->AddValue(1);
      g_syncer_metric->seconds_behind_master_metric_->SetValue(0);
      if (old_off < ph->log_pos) old_off = ph->log_pos;
      continue;
    }

    // Process by type
    // If this is a Rotate event, maybe it's the end of the requested binlog;
    // in this case we are done (stop transfer).
    // This is suitable for binlogs, not relay logs (but for now we don't read
    // relay logs remotely because the server is not able to do that). If one
    // day we read relay logs remotely, then we will have a problem with the
    // detection below: relay logs contain Rotate events which are about the
    // binlogs, so which would trigger the end-detection below.
    if (type == binary_log::ROTATE_EVENT) {
      if (/*!opt_print_debug_info*/ true)
        RDP_LOG_DBG << "rotate event: next pos=" << ph->log_pos <<
          ", time=" << ph->timestamp <<
          ", flag=" << ph->flag;

      // If this is a fake Rotate event, and not about our log, we can stop
      // transfer. If this a real Rotate event (so it's not about our log,
      // it's in our log describing the next log), we print it (because it's
      // part of our log) and then we will stop when we receive the fake one
      // soon.
      memcpy(log_file_name, (const char *)(event_buf + LOG_EVENT_HEADER_LEN +
                                           sizeof(uint64_t)),
             binlog_filename_size);
      log_file = log_file_name;

      // If this is a fake Rotate event, and not about our log, we can
      // stop transfer. If this a real Rotate event (so it's not about our log,
      // it's in our log describing the next log), we print it (because
      // it's part of our log) and then we will stop when we receive the fake
      // one soon.
      if (ph->timestamp == 0 || ph->log_pos == 0) {
        // rev->new_log_ident points a logfile to read. If the new file
        // is ahead the current log file, mysqlbinlog should read from the
        // first position of the new file so setting position (old_off)
        // to the head. If the new file is the same as current logfile,
        // it means fake rotate event was sent, and position should not be
        // changed.
        int c = strcmp(cur_logname, log_file);
        // switching to new file
        if (c < 0) {
          old_off = BIN_LOG_HEADER_SIZE;
          strncpy(cur_logname, log_file, binlog_filename_size);
        } else if (c > 0) {
          // Anyway, current log file should NOT in advance of log file
          // name extract from rotate log event ?
          RDP_LOG_ERROR << "cur log: " << cur_logname << " in advance of log_file: " << log_file;
          goto err_t;
        }

        // Reset the value of '# at pos' field shown against first event
        // of next binlog file (fake rotate) picked by mysqlbinlog --to-last-log
        event_len = 0;  // fake Rotate, so don't increment old_off
      }
    } else if (type == binary_log::FORMAT_DESCRIPTION_EVENT) {
      if (/*!opt_print_debug_info*/ true)
        RDP_LOG_DBG << "format event: next pos=" << ph->log_pos <<
          ", time=" << ph->timestamp <<
          ", flag=" << ph->flag;

      // The checksum algorithm current applied
      g_checksum_alg = Log_event_footer::get_checksum_alg(event_buf, event_len);
      RDP_LOG_DBG << "file " << cur_logname << "'s checksum is: " << 
              (g_checksum_alg != binary_log::BINLOG_CHECKSUM_ALG_OFF ? "CRC32" : "OFF");

      // Format description event needs to be written if:
      // - pos points the head of the binlog (pos == BIN_LOG_HEADER_SIZE)
      // - pos points --start-position and
      // current log equals to starting log file

      // When executing binlog dump after reconnect, it receives
      // Format description event but it should not be written because
      // it is not the beginning of the binlog.
      if (old_off == BIN_LOG_HEADER_SIZE) {  // A new file
        //binlog_writer->Rotate(cur_logname, true);
        //binlog_writer->Write((const char *)BINLOG_MAGIC, BIN_LOG_HEADER_SIZE,
        //                     false);
        ;
      } else {  // Append to exist file
        event_len = 0;
        //binlog_writer->Rotate(cur_logname);
      }
    }

    // To guarantee RDP's binlog is exactly the same with master,
    // ignore any binlog events with 'ARTIFICIAL' flag
    if (ph->flag &
        LOG_EVENT_ARTIFICIAL_F /*|| ph->flag_ & LOG_EVENT_IGNORABLE_F*/) {
      PrintLogEvent(ph, semi_sync_need_reply, true);
      continue;
    }

    // Master will always send Rotate/FDE/Previous GTID, in case of
    // RDP is update to master, we should filter out 'Previous GTID',
    // since it is already in this file
    if (ph->log_pos < old_off && ph->flag & LOG_EVENT_IGNORABLE_F) {
      PrintLogEvent(ph, semi_sync_need_reply, true);
      continue;
    }

    // We do not filter any event, it's real slave's duty
    if (0 != event_len && 0 != ph->log_pos && 0 != ph->timestamp) {
      if (!WriteBinlogAndReply(net, event_buf, event_len, cur_logname,
                               semi_sync_need_reply)) {
        goto err_t;
      }
    }

    if (old_off < ph->log_pos) old_off = ph->log_pos;

    // Once need reply, we should reset packet no. whatever the event type
    // For example, the following sequence:
    // 1. GTID  : SET
    // @@SESSION.GTID_NEXT='c6961562-f0e8-11e5-a9dc-3464a915af74:5' 2. Query :
    // flush error logs The reply flag always marked
    //
    // Note: ReplSemiSyncMaster::readSlaveReply()
    // @@ Master side only reset pky_nr = 1 when requires reply, so we catch up!
    if (/*type == binary_log::XID_EVENT && */ semi_sync_need_reply) {
      net->pkt_nr = 1;
    }
  }

  binlog_writer->Flush();
  return true;

err_t:
  binlog_writer->Flush();
  return false;
}

bool MySQLSlave::SlaveClearup(void) {
  if (mysql_) {
    mysql_close(mysql_);
    mysql_ = NULL;
  }

  return true;
}

// Binlog dump loop
void MySQLSlave::SyncBinlogs(void) {
  AppConfig *conf = g_syncer_app->app_conf_;
  int tmo = conf->GetValue<int>("retry.interval", 1);
  int mb_per_sec = conf->GetValue<int>("mysql.max.rate", 10);
  // checkpoint save last complete pkg transaction seq no, so +1
  CheckPoint *checkpoint = &rdp_comm::Singleton<CheckPoint>::GetInstance();
  g_trx_seqno = checkpoint->GetTransSeqNo() + 1;

  TimedWaiter waiter(tmo);

  // Setup net io rate limiter,
  // default is 10*1000 bytes/ms, equal to 10MB/s
  rate_limiter_ = new RateLimiter(mb_per_sec * 1000);

  while (retry_connect && signalhandler::IsRunning()) {
    waiter.Begin();
    // Expose the io thread state, 0 means connecting
    g_syncer_metric->io_thread_state_metric_->SetValue(0);

    if (!SlaveStartup()) goto err_t;
    if (!SlavePrepare()) goto err_t;
    if (!SlaveRequest()) goto err_t;
    if (!SlaveProcess()) goto err_t;
    if (!SlaveClearup()) goto err_t;

  err_t:
    if (retry_connect && signalhandler::IsRunning()) {
      // Expose the io thread state, -1 means disconnected
      g_syncer_metric->io_thread_state_metric_->SetValue(-1);
      g_syncer_app->Alarm("Connect to mysql master failed");
      waiter.End();
    }
    ++retry_counter;
  }
}

}  // namespace syncer
