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

#ifndef _SYNCER_LOGGER_H_
#define _SYNCER_LOGGER_H_

#include <stdarg.h>

#include <glog/logging.h>
#include <stdio.h>
#include <string>
#include <rdp-comm/util.h>

using rdp_comm::spinlock_t;
using rdp_comm::guard;
using rdp_comm::lock_guard;
using rdp_comm::GetLastErrorMsg;

namespace syncer {

// Simply writing log messages
class Logger {
 public:
  // Constructor
  explicit Logger(void);

  // Destructor
  ~Logger(void);

 public:
  // Log info to file, due to different CRT implementation of snprintf
  // the Log behavior is unsafe; especially on Windows!
  void Log(int line_no, const char *type, const char *fmt, ...);

  // Flush user space buffer to page cache
  void Flush(void) {
    if (NULL != log_file_) {
      fflush(log_file_);
    }
  }

  // Sync error log to disk
  void Sync(void) {
    if (NULL != log_file_) {
      fflush(log_file_);
      fdatasync(fileno(log_file_));
    }
  }

  // Close current file
  void Close(void) {
    if (NULL != log_file_) {
      fclose(log_file_);
      log_file_ = NULL;
    }
  }

 private:
  // Exit with dump error info to stderr
  inline void FatalError(const char *msg, int err) {
    fprintf(stderr, "%s failed: (%s). \n", msg, GetLastErrorMsg(err).c_str());
    fflush_unlocked(stderr);
    assert(0);
  }

  // Open a new file by name, remove previous file if
  // it had exceeded the defined maximum size
  void Open(bool remove_exist = false) {
    char name[512];
    this->Close();
    snprintf(name, sizeof(name), "%s/syncer.%04d%02d.log", "./log", year_,
             month_);
    if (remove_exist) {
      remove(name);
    }

    log_file_ = fopen(name, "ab");
    if (NULL == log_file_) {
      FatalError("fopen", errno);
    }

    struct stat st;
    if (0 != stat(name, &st)) {
      FatalError("stat", errno);
    }
    file_size_ = st.st_size;
  }

 private:
  // Log file
  FILE *log_file_;

 private:
  // Log instance logger
  spinlock_t lock_;

  // Year of current local time
  uint32_t year_;

  // Month of current local time
  uint32_t month_;

  // Size of current log file
  uint64_t file_size_;
};

}  // namespace syncer

namespace syncer {

//用Glogger 替换原来的log
class LoggerGoogleImpl {
 public:
  LoggerGoogleImpl(const std::string &sModuleName, const std::string &sLogPath,
                   const int iLogLevel);
  ~LoggerGoogleImpl();

  void LogError(const char *pcFormat, ...);

  void LogWarning(const char *pcFormat, ...);

  void LogInfo(const char *pcFormat, ...);

  void LogVerbose(const char *pcFormat, ...);

  char *LogString(int iLevel,const char *pcFormat, ...);
};
extern LoggerGoogleImpl *g_logger;


void LogBufInfo(const char *sOutBuf, int iLen,const char *msg);


#define RDP_LOG_DBG VLOG(1)

#define RDP_LOG_INFO LOG(INFO)
#define RDP_LOG_WARN LOG(WARNING)
#define RDP_LOG_ERROR LOG(ERROR)
#define RDP_LOG_FATAL LOG(FATAL)

}  // namespace syncer

#endif
