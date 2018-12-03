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

#ifndef RDP_COMM_METRIC_REPORTER_H
#define RDP_COMM_METRIC_REPORTER_H

#include <boost/circular_buffer.hpp>
#include <boost/function.hpp>

#include "comm_incl.h"
#include "threadbase.h"


namespace rdp_comm {


enum MetricValueType { 
  ValueTypeLong = 0, 
  // TODO: Support double type
  // ValueTypeDouble = 1 
};

enum EndpointType { 
  EndpointTypeApi = 0, 
  EndpointTypeHost, 
  EndpointTypeApp 
};

enum MetricType { 
  MetricTypeCounter = 0, 
  MetricTypeMeter, 
  MetricTypeAvg 
};



class Metric {
 public:

  // Create a Metic object, 
  Metric(const std::string& metric_name, MetricType metric_type, int duration) 
    : metric_name_(metric_name),
      metric_type_(metric_type),
      value_type_(ValueTypeLong),
      duration_(duration) {
    gettimeofday(&last_report_time_, NULL);
  }
  virtual ~Metric() {};

  const std::string& GetMetricName() { return metric_name_; }
  int GetDuration() { return duration_; }

  virtual int64_t GetLongValue() = 0;

  MetricValueType GetValueType() { return value_type_;  }
  
  void UpdateReportTime(const struct timeval cur_time) { 
    last_report_time_ = cur_time; 
  }

  // Return how many ms to wait to report this metric
  int TimeGap(const struct timeval* cur_time) { 
    int elapsed = GetIntervalMs(&last_report_time_, cur_time);
    return duration_ * 1000 - elapsed;
  }

 protected:

  std::string metric_name_;
  
  MetricType metric_type_;  
  MetricValueType value_type_;

  // Report intervals, in seconds
  int duration_;
  struct timeval last_report_time_;
};

class CounterMetric : public Metric {
 public:
  typedef boost::function<int64_t ()> Getter;

  CounterMetric(const std::string& metric_name, int duration)
    : Metric(metric_name, MetricTypeCounter, duration), 
    long_value_(0) {}

  void SetValue(int64_t value) {
    __atomic_exchange_n(&long_value_, value, __ATOMIC_SEQ_CST);
  }
  void AddValue(int64_t value) {
    __atomic_fetch_add(&long_value_, value, __ATOMIC_SEQ_CST);
  }

  int64_t GetLongValue() { 
    if (getter_) {
      int64_t value = getter_();
      __atomic_exchange_n(&long_value_, value, __ATOMIC_SEQ_CST);
      return value;
    }
    return __atomic_load_n(&long_value_, __ATOMIC_SEQ_CST);
  }

  void Use(const Getter& getter) { getter_ = getter; } ;
  
 private:
  int64_t long_value_;
  Getter getter_;
};


// AvgMetric can be used to calculate the avg value, 
// for example, the avg rt of the recently 1000 requests
class AvgMetric : public Metric {
 public:
  AvgMetric(const std::string& metric_name, int duration, size_t capacity) 
    : Metric(metric_name, MetricTypeAvg, duration), 
      buffer_(capacity) {}

  // Push a point in queue
  void PushValue(int64_t value) {
    lock_guard guard(&lock_);
    buffer_.push_back(value);
  }

  // Get avg value in the queue 
  int64_t GetLongValue() { 
    lock_guard guard(&lock_);
    int64_t total = 0;
    if (buffer_.size() == 0) {
      // Return -1 or 0?
      return 0;
    }

    boost::circular_buffer<int64_t>::iterator it= buffer_.begin();
    for (; it != buffer_.end(); ++it) {
      total += *it;
    }

    // Should the buffer be cleared?
    return total / buffer_.size();
  }

 private:

  spinlock_t lock_;

  boost::circular_buffer<int64_t> buffer_;
  
};

// MeterMetric can be used to calculate the speed of request, 
// for example, the tps of last 10s
class MeterMetric : public Metric {
 public:
  typedef boost::function<int64_t ()> Getter;

  MeterMetric(const std::string& metric_name, int duration) 
    : Metric(metric_name, MetricTypeMeter, duration),
      long_value_(0),
      pre_long_value_(0) {
    gettimeofday(&pre_time_, NULL);
  }

  void SetValue(int64_t value) {
    lock_guard guard(&lock_);
    long_value_ = value;  
  }

  void AddValue(int64_t value) {
    lock_guard guard(&lock_);
    long_value_ += value;  
  }

  int64_t GetLongValue() { 
    lock_guard guard(&lock_);
    int64_t ret;

    struct timeval cur_time;
    gettimeofday(&cur_time, NULL);

    int64_t interval = GetIntervalMs(&pre_time_, &cur_time) / 1000;

    if (interval == 0) {
      // Return -1 or 0?
      return 0;
    }
    if (getter_) {
      long_value_ = getter_();
    }
    ret = (long_value_ - pre_long_value_) / interval;
    
    pre_long_value_ = long_value_;
    pre_time_ = cur_time;

    return ret; 
  }

  void Use(const Getter& getter) { getter_ = getter; } ;

 private:

  spinlock_t lock_;

  int64_t long_value_;
  int64_t pre_long_value_;
  struct timeval pre_time_;

  Getter getter_;
  
};


class MetricsFile {
  public:
    MetricsFile(const std::string& dir, const std::string& basename,
        size_t roll_size, int days);
    ~MetricsFile();

    int Append(const char* logline, int len);
    void Flush();

  private:
    
    int RollFile();
    void DeleteOldFiles(time_t* now);
    std::string GetFileName(time_t* now);

    const std::string dir_;
    const std::string basename_;
    const size_t roll_size_;
    int days_;

    size_t written_bytes_;

    time_t last_roll_;
    FILE *file_;

};


class MetricReporter : public ThreadBase {
 public:
  explicit MetricReporter() 
  : file_(NULL) {
    exit_flag_ = false;
  }
  ~MetricReporter();

  int Init(const std::string& ns, const std::string& host, 
      const std::string& endpoint, const std::string& file_dir, 
      const std::string& file_name, size_t file_size, int days);

  inline void Exit(void) { exit_flag_ = true; }

  int RegistMetric(Metric* metric);
  void UnRegistMetric(Metric* metric);

 private:
  virtual void Run(void);


  int CheckNeedReportMetric(std::vector<std::string> &ready, const struct timeval* cur_time);


  /*
    Build json string like this:
    {
      "metricName": "inode_total",
      "endpoint": "gd9-mercury-kafka-001",
      "timestamp": 1493806423188,
      "namespace": "app",
      "longValue": 30324487,
      "meta": {
        "duration": 10,
        "endpointType": "app",
        "counterType": "gauge"
      }
     }
  */
  std::string BuildMetricString(Metric* metric, const timeval* cur_time);

  std::map<std::string, Metric*> metrics_;

  volatile bool exit_flag_;

  std::string namespace_;

  std::string host_;
  std::string endpoint_;

  MetricsFile *file_;
};


}  // namespace rdp_comm

#endif  // RDP_COMM_MONITOR_REPORT_H
