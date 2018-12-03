#include <iostream>

#include <gtest/gtest.h>
#include <glog/logging.h>
#include <gflags/gflags.h>

#include <boost/thread/thread.hpp>
#include <iostream>
#include <string>

#include <glog/logging.h>
#include <boost/lexical_cast.hpp>

#include "syncer_main.h"
#include "syncer_incl.h"
#include "syncer_conf.h"
#include "syncer_app.h"
#include "syncer_slave.h"
#include "logger.h"

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

::Sid_map *global_sid_map = NULL;
::Checkable_rwlock *global_sid_lock = NULL;

// Log compile timestamp to file
//static const char *startup_notice_ =
//    "-------------------------starting-------------------------";
static const char syncer_ver_[] =
    "[rdp_syncer 0.0.0.1] compile @("__DATE__
    " "__TIME__
    ")";

// Pointer to the Format_description_log_event of the currently active binlog.
// This will be changed each time a new Format_description_log_event is
// found in the binlog. It is finally destroyed at program termination.
static ::Format_description_log_event *glob_description_event = NULL;

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

// MySQLSlave* slave_ = nullptr;
// Main thread cleanup handler
static void CleanupHandler(void *arg) {
  LOG_INFO("Main thread cancelled.");

  if (app_logger_) {
    delete app_logger_;
    app_logger_ = nullptr;
  }

  if (g_syncer_app) {
    delete g_syncer_app;
    g_syncer_app = nullptr;
  }

  google::ShutdownGoogleLogging();
}
int iLogLevel = 0;
volatile int g_iStopFlag = 0;

void sig_handle(int signo) {
  printf("sig_handle:%d,set g_iStopFlag=1\n", signo);
  g_iStopFlag = 1;
  /*signal(SIGTERM, sig_handle);*/
  signal(signo, sig_handle);
}

void DaemonInit() {
  // int fd;
  // int iRet = 0 ;

  // shield some signals
  signal(SIGALRM, SIG_IGN);
  signal(SIGINT, SIG_IGN);
  signal(SIGHUP, SIG_IGN);
  signal(SIGQUIT, SIG_IGN);
  signal(SIGPIPE, SIG_IGN);
  signal(SIGTTOU, SIG_IGN);
  signal(SIGTTIN, SIG_IGN);
  signal(SIGCHLD, SIG_IGN);
  // signal(SIGTERM, SIG_IGN);
  signal(SIGTERM, sig_handle);

  signal(SIGUSR1, sig_handle);
  signal(SIGUSR2, sig_handle);

  // fork child process
  if (fork()) exit(0);

  // creates  a new session
  if (setsid() == -1) exit(1);

  // If you needn't STDIN,STDOUT,STDERR, close fd from 0;
  // for(fd = 3; fd < NOFILE; fd++)
  //    close(fd);
  // iRet = chdir("/");
  // umask(0);

  g_iStopFlag = 0;
  return;
}

int main(int argc, char **argv) {
  AppConfig *app_conf = NULL;
  std::string module_name = "rdp_syncer_dbg";
  std::string log_dir;

  std::string conf_file = "../conf/syncer.cfg";

  if (argc == 2) {
    conf_file = std::string(argv[1]);
  } else {
    printf("\nParam error!\n");
    printf("Usage:%s <CFGFileName> !\n", argv[0]);
    return -1;
  }

  g_syncer_app = new SyncerApp(conf_file);
  app_conf = g_syncer_app->Config();

  log_dir = app_conf->GetValue<std::string>("log.dir");
  iLogLevel = boost::lexical_cast<uint32_t>(
      app_conf->GetValue<std::string>("log.level"));

  // google::InitGoogleLogging("rdp_syncer");

  app_logger_ =
      new LoggerGoogleImpl(module_name, log_dir, iLogLevel);  // Logger();


  testing::InitGoogleTest(&argc, argv);

  gflags::ParseCommandLineFlags(&argc, &argv, false);

  g_syncer_app->Init();

  LOG(WARNING) << "Main thread RUN_ALL_TESTS.." << endl;

  return RUN_ALL_TESTS();

  
}

// We must include this here as it's compiled with different options for the server
#include "../strings/decimal.c"
#include "../sql/my_decimal.cc"
#include "../sql/log_event.cc"
#include "../sql/log_event_old.cc"
#include "../sql/rpl_utility.cc"
#include "../sql/rpl_gtid_sid_map.cc"
#include "../sql/rpl_gtid_misc.cc"
#include "../sql/rpl_gtid_set.cc"
#include "../sql/rpl_gtid_specification.cc"
#include "../sql/rpl_tblmap.cc"
