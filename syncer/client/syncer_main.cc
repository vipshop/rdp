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

#include <rdp-comm/logger.h>
#include "syncer_main.h"
#include "bounded_buffer.h"
#include "syncer_app.h"
#include "syncer_conf.h"
#include "syncer_incl.h"
#include "syncer_slave.h"
#include "parsing_coordinator.h"
#include "parsing_trx.h"
#include "encoded_msg_map.h"
#include "statistics_writer.h"
#include "memory_pool.h"
#include "metric_instanc.h"

using namespace syncer;

// Needed by net_serv.c
ulong bytes_sent = 0L;
ulong bytes_received = 0L;
ulong mysqld_net_retry_count = 10L;
ulong open_files_limit;
ulong opt_binlog_rows_event_max_size;
uint test_flags = 0;

static my_bool force_opt = 0;
static my_bool short_form = 0;
static ulong opt_server_id_mask = 0;
char server_version[SERVER_VERSION_LENGTH];

Sid_map *global_sid_map = NULL;
Checkable_rwlock *global_sid_lock = NULL;

static const char *module_name = "rdp_syncer";
// Log compile timestamp to file
static const char *startup_notice =
    "-------------------------starting-------------------------";
static const char syncer_ver[] =
    "[rdp_syncer 0.0.0.1] compile @("__DATE__
    " "__TIME__
    " git hash:"RDP_GIT_HASH
    ")";

// Pointer to the Format_description_log_event of the currently active binlog.
// This will be changed each time a new Format_description_log_event is
// found in the binlog. It is finally destroyed at program termination.
static Format_description_log_event *glob_description_event = NULL;

// Auxiliary function used by error() and warning().
// Prints the given text (normally "WARNING: " or "ERROR: "), followed
// by the given vprintf-style string, followed by a newline.
// @param format Printf-style format string.
// @param args List of arguments for the format string.
// @param msg Text to print before the string.
static void error_or_warning(const char *format, va_list args,
                             const char *msg) {
  fprintf(stderr, "%s: ", msg);
  vfprintf(stderr, format, args);
  fprintf(stderr, "\n");
}

// Prints a message to stderr, prefixed with the text "ERROR: "
// and suffixed with a newline.
// @param format Printf-style format string, followed by printf varargs.
static void error(const char *format, ...) {
  va_list args;
  va_start(args, format);
  error_or_warning(format, args, "ERROR");
  va_end(args);
}

// This function is used in log_event.cc to report errors.
// @param format Printf-style format string, followed by printf varargs.
static void sql_print_error(const char *format, ...) {
  va_list args;
  va_start(args, format);
  error_or_warning(format, args, "ERROR");
  va_end(args);
}

MySQLSlave *g_mysql_slave = NULL;

// Main thread cleanup handler
static void CleanupHandler(void *arg) {
  RDP_LOG_DBG << "Main thread cancelled.";
  if (g_mysql_slave) {
    delete g_mysql_slave;
    g_mysql_slave = NULL;
  }

  if (g_syncer_app) {
    delete g_syncer_app;
    g_syncer_app = NULL;
  }

  if (g_event_bounded_buffer) {
    delete g_event_bounded_buffer;
    g_event_bounded_buffer = NULL;
  }

  if (g_trx_bounded_buffer) {
    delete g_trx_bounded_buffer;
    g_trx_bounded_buffer = NULL;
  }

  if (g_logger) {
    delete g_logger;
    g_logger = nullptr;
  }

}

int main(int argc, char **argv) {
  pthread_cleanup_push(CleanupHandler, NULL);

  std::string cfg_file = "./syncer.cfg";
  std::string log_dir;
  int log_level;
  uint32_t trx_buffer_size;
  uint32_t encoded_msg_map_size;
  uint32_t check_thd_mt_rs_ms;

  if (argc == 2) {
    cfg_file = std::string(argv[1]);
  } else if (argc > 2) {
    std::cerr << "WARNING: Invalid command parameters!" << std::endl;
    goto quit_t;
  } else {
    std::cout << syncer_ver << std::endl;
    goto quit_t;
  }

  g_syncer_app = new SyncerApp(cfg_file);
  CreateMemoryPool();

  // Read configure items
  log_dir = g_syncer_app->app_conf_->GetValue<string>("log.dir", "../log/");
  log_level =
      g_syncer_app->app_conf_->GetValue<int>("log.level", google::GLOG_INFO);
  trx_buffer_size =
      g_syncer_app->app_conf_->GetValue<uint32_t>("trx.buffer.size", 50000);
  encoded_msg_map_size =
      g_syncer_app->app_conf_->GetValue<uint32_t>("msg.map.size");
  check_thd_mt_rs_ms =
      g_syncer_app->app_conf_->GetValue<uint32_t>("ckthdmtrs.time_ms");
  g_max_trans_size =
      g_syncer_app->app_conf_->GetValue<uint32_t>("trx.max_trans_size", 786432);

  // Set EncodedMsgMap capacity
  g_encode_msg_map.SetCapacity(encoded_msg_map_size);

  // Construct the logger
  g_logger = new LoggerGoogleImpl(module_name, log_dir, log_level);
  RDP_LOG_DBG << startup_notice;
  RDP_LOG_DBG << syncer_ver;

  if (g_syncer_app->Init()) {
    RDP_LOG_ERROR <<"Application init failed!";
    goto quit_t;
  }

  // Register main thread
  rdp_comm::signalhandler::StartSignalHandler(
      rdp_comm::signalhandler::MyQuitCoordinator, check_thd_mt_rs_ms);
  rdp_comm::signalhandler::AddThread(pthread_self(), "main.thread");

  g_event_bounded_buffer = new BoundedBuffer(5*trx_buffer_size);
  g_trx_bounded_buffer = new BoundedBuffer(trx_buffer_size);
  CreateMetricInstance();
  StartParsingCoordinator();

  g_mysql_slave = new MySQLSlave();
  g_mysql_slave->SyncBinlogs();

  // Quit coordinate from signal thread
  rdp_comm::signalhandler::Wait();
  StopParsingCoordinator();
  StopParsingTrx();

quit_t:
  pthread_cleanup_pop(1);
  std::cerr.flush();

  StatCloseAll();
  CloseMetricInstance();
  CloseMemoryPool();

  return 0;
}

// We must include this here as it's compiled with different options for the
// server
#include "../sql/log_event.cc"
#include "../sql/log_event_old.cc"
#include "../sql/my_decimal.cc"
#include "../sql/rpl_gtid_misc.cc"
#include "../sql/rpl_gtid_set.cc"
#include "../sql/rpl_gtid_sid_map.cc"
#include "../sql/rpl_gtid_specification.cc"
#include "../sql/rpl_tblmap.cc"
#include "../sql/rpl_utility.cc"
