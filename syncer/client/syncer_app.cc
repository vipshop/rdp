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

#include "syncer_main.h"
#include "syncer_progress.h"
#include "schema_manager.h"
#include "syncer_filter.h"
#include "syncer_app.h"
#include "util_sub_process.h"
#include "checkpoint.h"
#include "kafka_producer.h"
#include "encoded_msg_map.h"
#include "rebuild.h"
#include "util_sub_process.h"

#include <boost/lexical_cast.hpp>

using rdp_comm::MetricReporter;

namespace syncer {

SyncerApp::SyncerApp(const std::string &cfg_file) {
  app_conf_ = new AppConfig(cfg_file);
  slave_view_ = new SlaveView();
  synced_progress_ = new SyncedProgress(slave_view_);
  schema_manager_ = new SchemaManager();
  syncer_filter_ = new SyncerFilter();
  metric_reporter_ = new MetricReporter();
  zk_log_ = NULL;
}

SyncerApp::~SyncerApp(void) {
  if (syncer_filter_) {
    delete syncer_filter_;
    syncer_filter_ = nullptr;
  }

  if (slave_view_) {
    delete slave_view_;
    slave_view_ = nullptr;
  }

  if (schema_manager_) {
    delete schema_manager_;
    schema_manager_ = nullptr;
  }

  if (metric_reporter_) {
    delete metric_reporter_;
    metric_reporter_ = nullptr;
  }

  if (synced_progress_) {
    delete synced_progress_;
    synced_progress_ = nullptr;
  }

  if (app_conf_) {
    delete app_conf_;
    app_conf_ = nullptr;
  }

  if (zk_log_) {
    fclose(zk_log_);
    zk_log_ = NULL;
  }
}

// Initiate startup state
int SyncerApp::Init() {
  group_id_ = app_conf_->GetValue<string>(string("group.id"));

  std::string gtid_set;
  if (!InitZKProcess()) {
    return -1;
  }

  if (!InitLeaderElection()) {
    return -1;
  }

  rdp_comm::ZKProcess *zk_handle =
      &rdp_comm::Singleton<rdp_comm::ZKProcess>::GetInstance();

  rdp_comm::LeaderElection *leader =
      &rdp_comm::Singleton<rdp_comm::LeaderElection>::GetInstance();
  while (leader->IsFollower()) {
    sleep(1);
    // DRDP_LOG_INFO << "I am follower";
  }

  RDP_LOG_INFO << "I become leader";
  
  if (!InitMetricReporter()) {
    return -1;
  }

  if (!InitCheckPoint()) {
    return -1;
  }

  if (!InitProducer()) {
    return -1;
  }

  if (!InitSlaveView()) {
    return -1;
  }

  CheckPoint *checkpoint = &rdp_comm::Singleton<CheckPoint>::GetInstance();
  if (!checkpoint->GetGtidSet().empty()) {
    gtid_set.assign(checkpoint->GetGtidSet());
  }

  uint64_t epoch = checkpoint->GetEpoch();
  leader->SetEpoch(epoch);
  checkpoint->SetEpoch(leader->GetEpoch() + 1);

  RDP_LOG_INFO << "Syncer Process Start GTID Set: " << gtid_set;

  if (gtid_set.empty()) {
    synced_progress_->DownloadProgress();
  } else {
    // If specified gtid_set, use it as slave's gtid_executed
    if (!synced_progress_->SetGTIDSet((const uint8_t*)gtid_set.c_str(), gtid_set.length())) {
      RDP_LOG_ERROR << "Syncer Process Set Gtid: " << gtid_set << ", Failt";
      return -1;
    }
  }

  // immediately write zk when checkpoint thread start 
  if (!checkpoint->Start()) {
    RDP_LOG_ERROR << "Start CheckPoint Thread Failt";
    return -1;
  }

  RDP_LOG_INFO << "CheckPoint Thread Start";

  if (schema_manager_->Init()) {                                                                                                                        
    return -1;
  }

  if (syncer_filter_->Init(zk_handle)) {                                                                                                                        
    return -1;
  }


  return 0;
}

bool SyncerApp::InitZKProcess() {
  rdp_comm::ZKProcess *zk_handle =
      &rdp_comm::Singleton<rdp_comm::ZKProcess>::GetInstance();
  if (zk_handle->IsConnected()) return true;

  // Zookeeper log level
  string zk_log_level = app_conf_->GetValue<string>(string("zk.loglevel"));

  // Zookeeper hosts example: 127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183
  string zk_hosts = app_conf_->GetValue<string>(string("zk.hosts"));
  int zk_recv_time = app_conf_->GetValue<int>(string("zk.recvtimeout"));

  if (0 >= zk_recv_time) {
    RDP_LOG_WARN << "Configure File zk.recvtimeout Set Default:"
                 << ZK_DEFAULT_RECVTIMEOUT << " ms";
    zk_recv_time = ZK_DEFAULT_RECVTIMEOUT;
  }

  if (rdp_comm::CompareNoCase(zk_log_level, string("ERROR"))) {
    zk_handle->SetLogLevel(ZOO_LOG_LEVEL_ERROR);
  } else if (rdp_comm::CompareNoCase(zk_log_level, string("WARN"))) {
    zk_handle->SetLogLevel(ZOO_LOG_LEVEL_WARN);
  } else if (rdp_comm::CompareNoCase(zk_log_level, string("INFO"))) {
    zk_handle->SetLogLevel(ZOO_LOG_LEVEL_INFO);
  } else if (rdp_comm::CompareNoCase(zk_log_level, string("DEBUG"))) {
    zk_handle->SetLogLevel(ZOO_LOG_LEVEL_DEBUG);
  } else {
    zk_handle->SetLogLevel(ZOO_LOG_LEVEL_WARN);
  }

  // Zookeeper log dir
  string log_dir = app_conf_->GetValue<string>(string("log.dir"));

  // Zookeeper log, not use google log
  string log_name = app_conf_->GetValue<string>(string("zk.log.name"));
  size_t log_size = app_conf_->GetValue<size_t>("zk.log.size", 1024 * 1024 * 128);
  int log_days = app_conf_->GetValue<int>("zk.log.days", 7);

  if (!zk_handle->Initialize(zk_hosts, zk_recv_time, NULL, NULL, log_dir, log_name, log_size, log_days)) {
    RDP_LOG_ERROR << "Zookeeper Initialize Failt";
    return false;
  }

  // Connect Zookeeper
  if (!zk_handle->Connect()) {
    RDP_LOG_ERROR << "Connect To Zookeeper: " << zk_hosts << ", Failt";
    return false;
  }

  RDP_LOG_INFO << "Initialize ZKProcess Success";

  return true;
}

bool SyncerApp::InitSlaveView()
{
  int read_after_write = app_conf_->GetValue<int>("slaveview.read_after_write", 0);
  if (read_after_write) {
    string host = app_conf_->GetValue<std::string>("slaveview.master_ip", "");
    int port = app_conf_->GetValue<int>("slaveview.master_port", 0);
    if (host.empty()) {
      host = app_conf_->GetValue<std::string>("mysql.vip");
    }
    if (!port) {
      port = app_conf_->GetValue<int>("mysql.port");
    }

    string user = app_conf_->GetValue<std::string>("mysql.user");
    string password = app_conf_->GetValue<std::string>("mysql.password");
    string slave_server_ids = app_conf_->GetValue<std::string>("slaveview.slave_server_ids", "");

    int refresh_interval = app_conf_->GetValue<int>("slaveview.refresh_view_interval", 60*1000);
    int wait_timeout = app_conf_->GetValue<int>("slaveview.wait_timeout", 5*1000);
    int check_interval_idle = app_conf_->GetValue<int>("slaveview.check_slave_interval_idle", 10*1000);
    int check_interval_busy = app_conf_->GetValue<int>("slaveview.check_slave_interval_busy", 500);

    if (slave_view_->Init(host, port, user, password, slave_server_ids,
          refresh_interval, wait_timeout, check_interval_idle, check_interval_busy)) {
      RDP_LOG_ERROR << "Init slave view failed";
      return false;
    }
  }
  return true;
}

bool SyncerApp::InitLeaderElection() {
  rdp_comm::LeaderElection *leader =
      &rdp_comm::Singleton<rdp_comm::LeaderElection>::GetInstance();

  string host = app_conf_->GetValue<string>("syncer.host");
  string node_list_path = app_conf_->GetValue<string>(string("node.list.path"));
  string master_path = app_conf_->GetValue<string>(string("leader.path"));

  if (!leader->Initialize(host, node_list_path, master_path)) {
    RDP_LOG_ERROR << "Initialize Leader Selector Failt";
    return false;
  }

  return true;
}


bool SyncerApp::InitMetricReporter() {
  string ns = app_conf_->GetValue<string>("metrics.namespace", "app");
  string host = app_conf_->GetValue<string>("syncer.host");
  string endpoint = app_conf_->GetValue<string>("group.id");
  string file_dir = app_conf_->GetValue<string>("metrics.file.dir", "./");
  string file_name = app_conf_->GetValue<string>("metrics.file.name", "rdp_syncer");
  int file_size = app_conf_->GetValue<int>("metrics.file.size", 1024 * 1024); // Default 1M
  int days = app_conf_->GetValue<int>("metrics.file.days", 14); 
  if (metric_reporter_->Init(ns, host, endpoint, file_dir, file_name, file_size, days)) {
    return false;
  }

  return true;
}

bool SyncerApp::InitSchemaStore(string &gtid_set) {
    string script_dir = app_conf_->GetValue<string>("script.dir", "../scripts");
    string src_host = app_conf_->GetValue<std::string>("mysql.vip");
    int src_port = app_conf_->GetValue<int>("mysql.port");
    string src_user = app_conf_->GetValue<std::string>("mysql.user");
    string src_password = app_conf_->GetValue<std::string>("mysql.password");

    string dst_host = app_conf_->GetValue<string>("schema.meta.db.host");
    int dst_port = app_conf_->GetValue<int>("schema.meta.db.port");
    string dst_user = app_conf_->GetValue<string>("schema.meta.db.user");
    string dst_password = app_conf_->GetValue<string>("schema.meta.db.password", "");
    string dst_database = app_conf_->GetValue<string>("schema.meta.db.database");

    string cmd = "cd " + script_dir + " && ./init_schema_meta.sh" + 
      " --src-host=" + src_host + " --src-port=" + boost::lexical_cast<string>(src_port) +
      " --src-user=" + src_user + " --src-passwd=" + src_password + 
      " --dst-host=" + dst_host + " --dst-port=" + boost::lexical_cast<string>(dst_port) + 
      " --dst-user=" + dst_user + " --dst-passwd=" + dst_password + " --dst-database=" + dst_database + " --reset-dst";

    RDP_LOG_INFO << "Executing " << cmd;

    int status;
    StringWriter sw1;
    StringWriter sw2;
    SubProcess process;
    process.SetCommand("sh", 2, "-c", cmd.c_str());
    process.SetStdout(&sw1);
    process.SetStderr(&sw2);

    if (!process.Create()) {
      RDP_LOG_ERROR << "Create sub process failed :" << cmd;
      return false;
    }

    if (!process.Wait(&status)) {
      RDP_LOG_ERROR << "Wait sub process failed";
      return false;
    }

    if (status) {
      RDP_LOG_ERROR << "Process exits with return code: " << status 
        << ", err: " << sw2.AsString();
      return false;
    }

    json_error_t jerror;
    json_t *jobj = json_loads(sw1.AsString().c_str(), 0, &jerror);
    if (!jobj) {
      RDP_LOG_ERROR << "Decode json failed, json string:  " << sw1.AsString();
      return false;
    }
    json_t *jgtid = json_object_get(jobj, "gtid_binlog_pos");
    if (!jgtid) {
      json_decref(jobj);
      return false;
    }
    gtid_set = json_string_value(jgtid);
    json_decref(jobj);

  return true;
}

bool SyncerApp::InitCheckPoint() {
  CheckPoint *checkpoint = &rdp_comm::Singleton<CheckPoint>::GetInstance();
  rdp_comm::ZKProcess *zk_handle =
      &rdp_comm::Singleton<rdp_comm::ZKProcess>::GetInstance();

  string brokerlist = app_conf_->GetValue<string>(string("kafka.brokerlist"));
  string topic = app_conf_->GetValue<string>(string("kafka.topic"));
  uint32_t partition = app_conf_->GetValue<uint32_t>(string("kafka.partition"));
  string checkpoint_path = app_conf_->GetValue<string>(string("checkpoint.path"));
  uint32_t interval_ms = app_conf_->GetValue<uint32_t>(string("checkpoint.interval_ms"));
  uint32_t release_ms = app_conf_->GetValue<uint32_t>(string("checkpoint.release_mem_ms"), 100);
  string b_version_fback = app_conf_->GetValue<string>("kafka.b_version_fback");

  // default is "", not open librdkafka debug model
  string rdkafka_debug = app_conf_->GetValue<string>("kafka.debug", "");


  // 检查checkpoint节点是否存在，自动执行初始化脚本获取到gtid_set，并写入checkpoint节点
  struct Stat stat;
  string checkpoint_str;
  if (!zk_handle->GetData(checkpoint_path, checkpoint_str, &stat) || checkpoint_str.empty()) {
    RDP_LOG_WARN << "Zookeeper Path: " << checkpoint_path << " Not Exists, Will Create It";
    string gtid_set;
    if (!InitSchemaStore(gtid_set)) {
      return false;
    }
    struct Stat stat;
    string str = string("{\"flag\":\"1\",\"seq_no\":\"0\",\"gtid\":\"") + gtid_set + "\"}";
    RDP_LOG_INFO << "Checkpoint After Init Schema Store: " <<  str;
    if (!zk_handle->SetData(checkpoint_path, str, &stat, -1)) {
      return false;
    }
  }

  if (!checkpoint->Init(brokerlist, topic, zk_handle, checkpoint_path, interval_ms, release_ms)) {
    RDP_LOG_ERROR << "Initiate CheckPoint Failt";
    return false;
  }

  if (0 == checkpoint->GetZkFlag().compare("1")) {
    RDP_LOG_INFO << "CheckPoint Flag:" << checkpoint->GetZkFlag() << " Ignore Kafka Topic Data";
  } else {
    if (!Rebuild(checkpoint)) {
      // checkpoint记录offset为消息存储的当前位置，所以consumer开始消费位置需要+1
      uint64_t start_offset = 0;
      if (b_version_fback.compare(0, 3, "0.8") == 0) {
        uint32_t batch_num_msg = app_conf_->GetValue<uint32_t>("kafka.producer.batch_num_msg");
        start_offset = checkpoint->GetOffset() - batch_num_msg;
        if (start_offset <= 0)
          start_offset = 0;
      } else {
        start_offset = checkpoint->GetOffset() + 1;
      }
      RDP_LOG_ERROR << "Rebuild " << brokerlist << " topic:" << topic 
        << " Parition:" << partition << " Offset:" << start_offset << " Failt";
      return false;
    }
    RDP_LOG_INFO << "Rebuild Result: " << checkpoint->GetGtidSet() 
      << " Epoch:" << checkpoint->GetEpoch() 
      << " Trx Seq No:" << checkpoint->GetTransSeqNo() 
      << " Offset:" << checkpoint->GetOffset();
  }

  RDP_LOG_INFO << "CheckPoint Initiate Success";
  return true;
}

bool SyncerApp::InitProducer() {
  KafkaProducer *producer = &rdp_comm::Singleton<KafkaProducer>::GetInstance();
  CheckPoint *checkpoint = &rdp_comm::Singleton<CheckPoint>::GetInstance();

  string brokerlist = app_conf_->GetValue<string>("kafka.brokerlist");
  string topic = app_conf_->GetValue<string>("kafka.topic");
  uint32_t partition = app_conf_->GetValue<uint32_t>("kafka.partition");
  string required_ack = app_conf_->GetValue<string>("kafka.producer.acks");
  uint32_t batch_num_msg =
      app_conf_->GetValue<uint32_t>("kafka.producer.batch_num_msg");
  uint32_t batch_time_ms =
      app_conf_->GetValue<uint32_t>("kafka.producer.qbuf_max_ms");
  string msg_max_bytes =
      app_conf_->GetValue<string>("kafka.producer.msg_max_bytes");
  vector<int> partitions;
  partitions.push_back(partition);

  uint32_t q_buf_max_msgs =
      app_conf_->GetValue<uint32_t>("kafka.producer.q_buf_max_msgs", 100000);
  uint32_t q_buf_max_kb =
      app_conf_->GetValue<uint32_t>("kafka.producer.q_buf_max_kb", 2000000);
  int pop_map_msgs =
      app_conf_->GetValue<int>("kafka.producer.pop_map_msgs", 20);

  string b_version_fback = app_conf_->GetValue<string>("kafka.b_version_fback");

  string send_max_retry = app_conf_->GetValue<string>("kafka.producer.send_max_retry", "0");

  // default is false, if librdkafka sock close, not exit program
  string log_cn_close = app_conf_->GetValue<string>("kafka.producer.log_cn_close", "false");

  // default is "", not open librdkafka debug model
  string rdkafka_debug = app_conf_->GetValue<string>("kafka.debug", "");

  uint32_t no_rsp_timeout_ms = app_conf_->GetValue<uint32_t>("kafka.no_rsp_timeout_ms", 10 * 1000);
  uint32_t metadata_refresh_interval_ms = app_conf_->GetValue<uint32_t>("kafka.metadata_refresh_interval_ms", 30 * 1000);

  if (!producer->Init(brokerlist, topic, partitions, required_ack,
                      batch_num_msg, batch_time_ms, msg_max_bytes, pop_map_msgs,
                      q_buf_max_msgs, q_buf_max_kb, b_version_fback,
                      send_max_retry, log_cn_close, rdkafka_debug, no_rsp_timeout_ms,
                      metadata_refresh_interval_ms,
                      &g_encode_msg_map, checkpoint)) {
    RDP_LOG_ERROR << "Initiate Kafka Producer Failt";
    return false;
  }

  if (!producer->Start()) {
    RDP_LOG_ERROR << "Start Kafka Producer Thread Failt";
    return false;
  }

  RDP_LOG_INFO << "Producer Thread Start";

  return true;
}

void SyncerApp::Alarm(const string& message) {
  string output;
  string script_dir = app_conf_->GetValue<string>("script.dir", "../scripts");
  string cmd = script_dir + "/alarm.sh '[" +group_id_+"]" + message +"'";

  int try_times = 0;
  while(try_times++ < 5) {
    if (try_times != 0) {
      sleep(5);
    }
    RDP_LOG_WARN << "Executing cmd: " << cmd;

    int rc = 0;
    SubProcess process;
    process.SetCommand(script_dir+"/alarm.sh", 1, message.c_str());
    if (!process.Output(&rc, output)) {
      RDP_LOG_ERROR << "Execute failed";
      continue;
    }
    RDP_LOG_WARN  << "Execute result: " << output;

    if (rc) {
      RDP_LOG_ERROR << "Alarm failed";
      continue;
    }

    json_error_t jerror;
    json_t *jobj = json_loads(output.c_str(), 0, &jerror);
    if (!jobj) {
      RDP_LOG_ERROR << "Alarm failed";
      continue;
    }

    json_t *jcode = json_object_get(jobj, "code");
    if (jcode) {
      rc = json_integer_value(jcode);
    }

    if (!jcode || rc) {
      RDP_LOG_ERROR << "Alarm failed";
      json_decref(jobj);
      continue;
    }

    RDP_LOG_WARN  << "Alarm succeed";
    break;
  }
}

SyncerApp *g_syncer_app;

}  // namespace syncer
