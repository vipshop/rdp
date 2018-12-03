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

#include "util.h"

namespace rdp_comm {

const char *ErrorStr(int code) {
  switch (code) {
    case ZOK:
      return "Everything Is OK";

    case ZSYSTEMERROR:
      return "System Error";

    case ZRUNTIMEINCONSISTENCY:
      return "A Runtime Inconsistency Was Found";

    case ZDATAINCONSISTENCY:
      return "A Data Inconsistency Was Found";

    case ZCONNECTIONLOSS:
      return "Connection To The Server Has Been Lost";

    case ZMARSHALLINGERROR:
      return "Error While Marshalling Or Unmarshalling Data";

    case ZUNIMPLEMENTED:
      return "Operation Is Unimplemented";

    case ZOPERATIONTIMEOUT:
      return "Operation Timeout";

    case ZBADARGUMENTS:
      return "Invalid Arguments";

    case ZINVALIDSTATE:
      return "Invalid Zhandle State";

    case ZAPIERROR:
      return "Api Error";

    case ZNONODE:
      return "Node Does Not Exist";

    case ZNOAUTH:
      return "Not Authenticated";

    case ZBADVERSION:
      return "Version Conflict";

    case ZNOCHILDRENFOREPHEMERALS:
      return "Ephemeral Nodes May Not Have Children";

    case ZNODEEXISTS:
      return "The Node Already Exists";

    case ZNOTEMPTY:
      return "The Node Has Children";

    case ZSESSIONEXPIRED:
      return "The Session Has Been Expired By The Server";

    case ZINVALIDCALLBACK:
      return "Invalid Callback Specified";

    case ZINVALIDACL:
      return "Invalid ACL Specified";

    case ZAUTHFAILED:
      return "Client Authentication Failed";

    case ZCLOSING:
      return "ZooKeeper Is Closing";

    case ZNOTHING:
      return "(Not Error) No Server Responses To Process";

    case ZSESSIONMOVED:
      return "Session Moved To Another Server, So Operation Is Ignored";

    default:
      return "Unknown Error";
  }
}

const char *EventStr(int event) {
  if (ZOO_CREATED_EVENT == event)
    return "ZOO_CREATED_EVENT";
  else if (ZOO_DELETED_EVENT == event)
    return "ZOO_DELETED_EVENT";
  else if (ZOO_CHANGED_EVENT == event)
    return "ZOO_CHANGED_EVENT";
  else if (ZOO_SESSION_EVENT == event)
    return "ZOO_SESSION_EVENT";
  else if (ZOO_NOTWATCHING_EVENT == event)
    return "ZOO_NOTWATCHING_EVENT";
  else
    return "Unknown Event";
}

const char *StateStr(int state) {
  if (ZOO_EXPIRED_SESSION_STATE == state)
    return "ZOO_EXPIRED_SESSION_STATE";
  else if (ZOO_AUTH_FAILED_STATE == state)
    return "ZOO_AUTH_FAILED_STATE";
  else if (ZOO_CONNECTING_STATE == state)
    return "ZOO_CONNECTING_STATE";
  else if (ZOO_ASSOCIATING_STATE == state)
    return "ZOO_ASSOCIATING_STATE";
  else if (ZOO_CONNECTED_STATE == state)
    return "ZOO_CONNECTED_STATE";
  else
    return "Unknown State";
}

std::string ParentPath(const std::string &path) {
  if (path.empty()) return "";
  //
  size_t pos = path.rfind('/');
  if (path.length() - 1 == pos) {
    // skip the tail '/'
    pos = path.rfind('/', pos - 1);
  }
  if (std::string::npos == pos) {
    return "/";  //  parent path of "/" is also "/"
  } else {
    return path.substr(0, pos);
  }
}

std::string StrTime(time_t *t) {
  static char buf[32] = {0x0};
  if (NULL == t) {
    time_t tt;
    time(&tt);
    struct tm *a;
    a = localtime(&tt);
    snprintf(buf, sizeof(buf) - 1, "%04d%02d%02d-%02d:%02d:%02d",
             1900 + a->tm_year, a->tm_mon + 1, a->tm_mday, a->tm_hour,
             a->tm_min, a->tm_sec);
    return std::string(buf, strlen(buf));
  }
  struct tm *a;
  a = localtime(t);
  snprintf(buf, sizeof(buf) - 1, "%04d%02d%02d-%02d:%02d:%02d",
           1900 + a->tm_year, a->tm_mon + 1, a->tm_mday, a->tm_hour, a->tm_min,
           a->tm_sec);
  return std::string(buf, strlen(buf));
}

int64_t CharToInt64(const char *p) {
  if (NULL == p) return 0;
  std::stringstream strValue;
  strValue << p;
  int64_t rs = 0;
  strValue >> rs;
  return rs;
}

uint64_t GetIntervalMs(const struct timeval *begin, const struct timeval *end) {
  assert(NULL != begin && NULL != end);
  return (end->tv_sec - begin->tv_sec) * 1000 +
         (end->tv_usec - begin->tv_usec) / 1000;
}

uint64_t GetIntervalUs(const struct timeval *begin, const struct timeval *end) {
  assert(NULL != begin && NULL != end);
  return (end->tv_sec - begin->tv_sec) * 1000 * 1000 +
         (end->tv_usec - begin->tv_usec);
}

void SplitString(const std::string &source, const char *sp,
                 std::vector<std::string> *rs) {
  assert(NULL != rs);
  size_t begin = 0;
  int sp_len = strlen(sp);
  size_t end = source.find_first_of(sp, begin);
  while (end != std::string::npos) {
    rs->push_back(source.substr(begin, end - begin));
    begin = end + sp_len;
    end = source.find_first_of(sp, begin);
  }
  if ((end - begin) > 0) {
    rs->push_back(source.substr(begin, end - begin));
  }
}

// Return the error message
std::string GetLastErrorMsg(int err) {
  char err_msg[128] = {0x0};
  return std::string(strerror_r(err, err_msg, sizeof(err_msg)));
}

std::string StrToLower(std::string t) {
  std::string rs = t;
  std::transform(t.begin(), t.end(), rs.begin(), (int (*)(int))std::tolower);
  return rs;
}

bool CompareNoCase(const std::string &a, const std::string &b) {
  if (0 == StrToLower(a).compare(StrToLower(b)))
    return true;
  else
    return false;
}

int CheckThreadAlive(pthread_t tid) {
  int ret = 0;
  ret = pthread_kill(tid, 0);
  if (ESRCH == ret) {
    LOG(INFO) << "Thread: " << tid << " Not Exists";
    return 1;
  }

  if (EINVAL == ret) {
    LOG(ERROR) << "An Invalid Signal Was Specified";
    return -1;
  }

  char buffer[32];  // pthread_setname_np name len is less then 15
  memset(buffer, 0, sizeof(buffer));
  ret = pthread_getname_np(tid, buffer, sizeof(buffer));
  if (0 == ret) {
    DLOG(INFO) << "Thread: " << tid << ", Alive: " << buffer;
    return 0;
  } else if (ESRCH == ret) {
    LOG(INFO) << "Thread: " << tid << " Not Exists";
    return 1;
  } else {
    LOG(ERROR) << "Thread: " << tid << " Get Name Error: " << ret;
    return -1;
  }

  return 0;
}

uint64_t TimevalToUs(const struct timeval &t) {
  return (t.tv_sec * 1000 * 1000 + t.tv_usec);
}

std::string FormatTimeval(struct timeval *tv) {
  ssize_t written = -1;
  struct tm local_tm;
  localtime_r(&tv->tv_sec, &local_tm);

  char buffer[32] = {0};
  written = (ssize_t)strftime(buffer, sizeof(buffer), "%Y-%m-%dT%H:%M:%S", &local_tm);
  snprintf(buffer + written, sizeof(buffer) - (size_t)written, ".%06dZ", tv->tv_usec);

  return std::string(buffer);
}

}  // namespace rdp_comm
