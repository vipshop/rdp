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

#include <map>
#include <string>
#include <vector>
#include <sys/types.h>
#include <dirent.h>

#include "metric_reporter.h"

using std::string;
using std::vector;
using std::map;

namespace rdp_comm {

MetricReporter::~MetricReporter() {
  if (file_ != NULL) {
    delete file_;
  }
}

int MetricReporter::Init(const string& ns, const string& host, 
    const string& endpoint, const string& dir, const string &name, 
    size_t size, int days) {
  namespace_ = ns;
  host_ = host;
  endpoint_ = endpoint;
  file_ = new MetricsFile(dir, name, size, days);
  if (file_ == NULL) {
    return -1;
  }

  if (Start() == false) {
    return -1;
  }

  return 0;
}


int MetricReporter::RegistMetric(Metric* metric) {
  // Regist is called from main thread, not need to lock
  string metric_name = metric->GetMetricName();
  map<string, Metric*>::iterator it = metrics_.find(metric_name);
  if (it != metrics_.end()) {
    return -1;
  }

  metrics_[metric_name] = metric;

  // TODO: Wake up the thread
  return 0;
}

void MetricReporter::UnRegistMetric(Metric* metric) {
  // UnRegist is called from main thread, not need to lock
  string metric_name = metric->GetMetricName();
  map<string, Metric*>::iterator it = metrics_.find(metric_name);
  if (it == metrics_.end()) {
    return;
  }

  metrics_.erase(metric_name);

  return;
}

int MetricReporter::CheckNeedReportMetric(vector<string>& ready, const struct timeval* cur_time) {
  // Sleep 500ms at most
  int min_gap = 500;
  map<string, Metric*>::iterator it = metrics_.begin();
  for (; it != metrics_.end(); ++it) {
    int gap = (it->second)->TimeGap(cur_time);
    gap = gap>0 ? gap : 0;
    if (gap == 0) {
      // Need to report
      ready.push_back(it->first);
    }
    if (gap < min_gap) {
      min_gap = gap;
    }
  }
  return min_gap;
}

string MetricReporter::BuildMetricString(Metric* metric, const struct timeval* cur_time) {
  string report_body;
  uint64_t timestamp = cur_time->tv_sec * 1000;

  json_t* json = json_object();

  json_object_set_new(json, "metricName", json_string(metric->GetMetricName().c_str()));
  json_object_set_new(json, "timestamp", json_integer(timestamp));
  json_object_set_new(json, "namespace", json_string(namespace_.c_str()));
  json_object_set_new(json, "hostname", json_string(host_.c_str()));
  json_object_set_new(json, "endpoint", json_string(endpoint_.c_str()));

  if (metric->GetValueType() == ValueTypeLong) {
    json_object_set_new(json, "longValue", json_integer(metric->GetLongValue()));
  } else {
    // TODO:
  }

  json_t* json_meta = json_object();
  json_object_set_new(json_meta, "duration", json_integer(metric->GetDuration()));
  json_object_set_new(json_meta, "endpointType", json_string("app"));
  json_object_set_new(json_meta, "counterType", json_string("gauge"));
  json_object_set_new(json, "meta", json_meta);

  char* result = json_dumps(json, 0);

  report_body.append(result).append("\n");

  json_decref(json);
  free(result);
  
  return report_body;
}

void MetricReporter::Run(void) {
  pthread_setname_np(pthread_self(), "reporter.thread");

  exit_flag_ = false;
  while (!exit_flag_) {
    struct timeval cur_time;
    gettimeofday(&cur_time, NULL);

    vector<string> ready;
    int gap = CheckNeedReportMetric(ready, &cur_time);
    // LOG(INFO) << "Need to wait " << gap << "ms to report";
    if (gap) {
      // TODO: Use timer_fd
      usleep(gap * 1000);
      continue;
    }
   

    for (size_t i=0; i<ready.size(); i++) {
      const string key = ready[i];
      Metric* metric = metrics_[key];
      string str = BuildMetricString(metric, &cur_time);
      if (file_->Append(str.c_str(), str.size())) {
        LOG(ERROR) << "Append metric failed: " << key;
      }
      metric->UpdateReportTime(cur_time);
    }

    file_->Flush();
  }
}


MetricsFile::MetricsFile(const string& dir, const string& basename, size_t roll_size, int days)
  : dir_(dir),
    basename_(basename),
    roll_size_(roll_size),
    days_(days),
    written_bytes_(0),
    last_roll_(0),
    file_(NULL) {
  assert(basename.find('/') == string::npos);
  assert(0 == RollFile());
}

MetricsFile::~MetricsFile() {
  if (file_) {
    fclose(file_);
  }
}

int MetricsFile::Append(const char* line, int len) {
  if (1 != fwrite(line, len, 1, file_)) {
      return -1;  
  }
  written_bytes_ += len;
  if (written_bytes_ > roll_size_) {
    if (RollFile()) {
      return -1;
    }
  }
  return 0;
}

void MetricsFile::Flush() {
  fflush(file_);
}

void MetricsFile::DeleteOldFiles(time_t* now) {
  static int suffix_len = strlen(".metrics");

  DIR* dir = opendir(dir_.c_str());
  if (dir == NULL) {
    return;
  }
  struct dirent* entry;
  while ( (entry = readdir(dir))) {
    if (entry->d_type != DT_REG) {
      continue;
    }
    if (strncmp(entry->d_name, basename_.c_str(), basename_.size()) != 0) {
      continue;
    }

    int len = strlen(entry->d_name);
    if (strncmp(&(entry->d_name[len-suffix_len]), ".metrics", suffix_len) != 0) {
      continue;
    }

    string path = dir_ + '/' + entry->d_name;
    struct stat sb;
    if (lstat(path.c_str(), &sb)) {
      continue;
    }

    if (difftime(sb.st_mtime, *now) < -3600*24*days_) {
      unlink(path.c_str());
    }

  }
}

int MetricsFile::RollFile() {
  time_t now = 0;
  now = time(NULL);
  string new_filename = dir_ + '/' + GetFileName(&now) + ".metrics";
  string src_filename = dir_ + '/' + basename_ + ".metrics";

  if (now > last_roll_) {
    // Delete old files
    DeleteOldFiles(&now);
    last_roll_ = now;

    if (file_) {
      fclose(file_);
    }
    file_ = NULL;
   
    // Move src_filename to a new filename 
    if (rename(src_filename.c_str(), new_filename.c_str())) {
      if (errno != ENOENT) {
        return -1;
      }
    }

    file_ = fopen(src_filename.c_str(), "w+b");
    if (!file_) {
      return -1;
    }

    written_bytes_ = 0;
  }

  return 0;
}

string MetricsFile::GetFileName(time_t* now) {
  string filename;
  filename = basename_;

  char timebuf[32];
  struct tm tm;
  gmtime_r(now, &tm); 
  strftime(timebuf, sizeof timebuf, ".%Y%m%d%H%M%S", &tm);
  filename += timebuf;

  return filename;
}


}
