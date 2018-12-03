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

#ifndef _SYNCER_APP_H_
#define _SYNCER_APP_H_

#include "syncer_conf.h"

#include <rdp-comm/util.h>
#include <rdp-comm/zk_process.h>
#include <rdp-comm/zk_config.h>
#include <rdp-comm/leader_election.h>
#include <rdp-comm/signal_handler.h>
#include <rdp-comm/metric_reporter.h>
#include <rdp-comm/logger.h>
#include "syncer_progress.h"
#include "schema_manager.h"
#include "syncer_filter.h"
#include "slave_view.h"

namespace syncer {

struct SyncerApp {
  explicit SyncerApp(const std::string &cfg_file);
  ~SyncerApp(void);

  // Initiate startup state
  int Init();

  // Initiate ZKProcess
  bool InitZKProcess(void);

  // Initiate Leader Election
  bool InitLeaderElection(void);

  // Initiate ZkConfig
  bool InitZkConfig(void);

  // Initiate SchemaStore
  bool InitSchemaStore(std::string& gtid_set);

  // Initiate CheckPoint
  bool InitCheckPoint(void);

  // Initiate Producer
  bool InitProducer(void);

  bool InitSlaveView(void);

  bool InitMetricReporter(void);

  void Alarm(const std::string& message);

  AppConfig *app_conf_;
  SyncedProgress *synced_progress_;
  SchemaManager *schema_manager_;
  SyncerFilter *syncer_filter_;
  SlaveView *slave_view_;

  rdp_comm::MetricReporter *metric_reporter_;

  // zookeeper log file handle
  FILE *zk_log_;

  string group_id_;
};

extern SyncerApp *g_syncer_app;

}  // namespace syncer

#endif  // _SYNCER_APP_H_
