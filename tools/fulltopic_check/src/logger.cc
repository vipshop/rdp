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

#include "logger.h"

#include <boost/thread/lock_guard.hpp>
#include <boost/thread/mutex.hpp>
#include <string>

using namespace std;

namespace syncer {

// Maximum size of each log file
static const uint64_t MAX_LOG_FILE_SIZE = (256 << 20);
static const int32_t MAX_LOG_LINE_SIZE = 512;

// String format of month
static const char *MONTHS[] = {"Jan", "Feb", "Mar", "Apr", "May", "Jun",
                               "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};

// Constructor
Logger::Logger(void) : log_file_(NULL) {
  // Create folder "./log" if it does NOT exist
  if (0 != access("../log", R_OK)) {
    mkdir("../log", S_IRWXU);
  }

  // Current time-stamp
  time_t cur_time = 0;
  time(&cur_time);
  struct tm now;
  localtime_r(&cur_time, &now);
  year_ = 1900 + now.tm_year;
  month_ = now.tm_mon + 1;

  this->Open();
}

// Destructor
Logger::~Logger(void) { this->Close(); }

// Log info to file, due to different CRT implementation of snprintf
// the Log behavior is unsafe; especially on Windows!
void Logger::Log(int line_no, const char *type, const char *fmt, ...) {
  char tmp[MAX_LOG_LINE_SIZE + 1];

  time_t cur_time;
  time(&cur_time);
  struct tm now;
  localtime_r(&cur_time, &now);
  uint32_t year = 1900 + now.tm_year;
  uint32_t month = now.tm_mon + 1;

  // print fixed size header
  int ret = snprintf(
      tmp, sizeof(tmp), "<%02d%s%04d %02d:%02d:%02d> [%016lu] [%05d] [%s]: ",
      now.tm_mday, MONTHS[now.tm_mon], now.tm_year + 1900, now.tm_hour,
      now.tm_min, now.tm_sec, (unsigned long int)pthread_self(), line_no, type);

  if (ret < 0) {
    FatalError("snprintf", errno);
  }
  int left = sizeof(tmp) - ret;
  int write_bytes = ret;

  // variable part of whole message
  va_list ap;
  va_start(ap, fmt);
  ret = vsnprintf(tmp + ret, left, fmt, ap);
  if (ret < 0) {
    FatalError("vsnprintf", errno);
  }
  va_end(ap);

  write_bytes += ret;
  if (write_bytes >= MAX_LOG_LINE_SIZE) {
    tmp[MAX_LOG_LINE_SIZE - 1] = '\n';
    tmp[MAX_LOG_LINE_SIZE] = 0x0;
    write_bytes = MAX_LOG_LINE_SIZE;
  } else {
    tmp[write_bytes] = '\n';
    tmp[write_bytes + 1] = 0x0;
    ++write_bytes;
  }

  // Append write to log file is thread-safe, but we should
  // check and redirect to new file if necessary.
  lock_guard guard(&lock_);

  // Year or month number has been changed,
  // So close old file and create a new one
  if ((year != year_) || (month != month_) || file_size_ >= MAX_LOG_FILE_SIZE) {
    year_ = year;
    month_ = month;
    this->Open((file_size_ >= MAX_LOG_FILE_SIZE));
  }

  fwrite(tmp, write_bytes, 1, log_file_);
  file_size_ += write_bytes;
}

boost::mutex glog_mutex;

LoggerGoogleImpl::LoggerGoogleImpl(const std::string &sModuleName,
                                   const std::string &sLogPath,
                                   const int iLogLevel) {
  google::InitGoogleLogging(sModuleName.c_str());

  FLAGS_stderrthreshold = google::INFO;
  FLAGS_logtostderr = true;
  FLAGS_log_prefix = false;

  FLAGS_log_dir = sLogPath;
  switch (iLogLevel) {
    case 1:
      FLAGS_minloglevel = google::ERROR;
      break;
    case 2:
      FLAGS_minloglevel = google::WARNING;
      break;
    case 3:
      FLAGS_minloglevel = google::INFO;
      break;
    default:
      FLAGS_minloglevel = google::INFO;
  }

  LogError("%d,%d,%s", FLAGS_minloglevel, google::ERROR,
           "init_glog_error_file");
  LogWarning("%d,%d,%s", FLAGS_minloglevel, google::WARNING,
             "init_glog_warning_file");
  LogInfo("%d,%d,%s", FLAGS_minloglevel, google::INFO, "init_glog_info_file");
  LogVerbose("%d,%d,%s", FLAGS_minloglevel, google::INFO,
             "init_glog_verbose_file");
}

LoggerGoogleImpl::~LoggerGoogleImpl() { google::ShutdownGoogleLogging(); }

void LoggerGoogleImpl::LogError(const char *pcFormat, ...) {
  char sBuf[1024] = {0};
  string newFormat = "\033[41;37m " + string(pcFormat) + " \033[0m";
  va_list args;
  va_start(args, pcFormat);
  vsnprintf(sBuf, sizeof(sBuf), newFormat.c_str(), args);
  va_end(args);

  //RDP_LOG_ERROR << sBuf;
}

void LoggerGoogleImpl::LogWarning(const char *pcFormat, ...) {
  char sBuf[1024] = {0};
  string newFormat = "\033[44;37m " + string(pcFormat) + " \033[0m";
  va_list args;
  va_start(args, pcFormat);
  vsnprintf(sBuf, sizeof(sBuf), newFormat.c_str(), args);
  va_end(args);

  //LOG(WARNING) << sBuf;
}

char *LoggerGoogleImpl::LogString(int iLevel, const char *pcFormat, ...) {
  if (FLAGS_minloglevel <= iLevel) {
    boost::lock_guard<boost::mutex> oLock(glog_mutex);
    static char sBuf[1024] = {0};
    va_list args;
    va_start(args, pcFormat);
    vsnprintf(sBuf, sizeof(sBuf), pcFormat, args);
    va_end(args);
    return sBuf;
  } else {
    static char sBuf[2] = {0};
    return sBuf;
  }
}
void LoggerGoogleImpl::LogInfo(const char *pcFormat, ...) {
  char sBuf[1024] = {0};
  string newFormat = "\033[45;37m " + string(pcFormat) + " \033[0m";
  va_list args;
  va_start(args, pcFormat);
  vsnprintf(sBuf, sizeof(sBuf), newFormat.c_str(), args);
  va_end(args);

  //RDP_LOG_INFO << sBuf;
}

void LoggerGoogleImpl::LogVerbose(const char *pcFormat, ...) {
  char sBuf[1024] = {0};
  va_list args;
  va_start(args, pcFormat);
  vsnprintf(sBuf, sizeof(sBuf), pcFormat, args);
  va_end(args);

  //RDP_LOG_INFO << sBuf;
}

LoggerGoogleImpl *g_logger = NULL;  // new LoggerGoogleImpl();

#define COL_TITLE "______00_01_02_03_04_05_06_07_08_09_0A_0B_0C_0D_0E_0F\n"

int HexDumpStr(const char *data, long size, unsigned char *sOutStr) {
  long i;
  long x;
  int r = 1, offset = 0, len;
  char d[32];
  unsigned char bfr[64];

  for (i = 0, x = 0;; i++) {
    if (i < size) {
      if (x == 0) {
        len = sprintf((char *)&(sOutStr[offset]), "%04lx: ", i);
        offset += len;
      }
      snprintf(d, 9, "%08x", data[i]);
      len = sprintf((char *)&(sOutStr[offset]), "%c%c ", d[6], d[7]);
      offset += len;
      bfr[x] = data[i];
      if (bfr[x] < 0x20) bfr[x] = '.';
      if (bfr[x] > 0x7f) bfr[x] = '.';
    } else {
      if (x == 0)
        break;
      else {
        len = sprintf((char *)&(sOutStr[offset]), "   ");
        offset += len;
        bfr[x] = ' ';
        r = 0;
      }
    }
    x++;
    if (!(x < 16)) {
      bfr[x] = 0;
      len = sprintf((char *)&(sOutStr[offset]), "%s\n", bfr);
      offset += len;
      x = 0;
      if (!r) break;
    }
  }
  return offset;
}

void LogBufInfo(const char *sOutBuf, int iLen, const char *msg) {
  if (FLAGS_minloglevel == google::INFO) {
    static unsigned char sHexBuf[1000000];
    int iBufLen = 0;
    if (iLen > 20000) {
      iBufLen = 20000;
    } else {
      iBufLen = iLen;
    }
    HexDumpStr(sOutBuf, iBufLen, sHexBuf);

    RDP_LOG_INFO << msg << ":\n" << sHexBuf << std::endl;
  }

  return;
}

// Logger *g_logger = nullptr;

}  // namespace syncer
