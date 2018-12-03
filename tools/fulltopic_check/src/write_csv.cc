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

#include "write_csv.h"

#include <stdio.h>
#include <time.h>
#include <iostream>
#include <errno.h>
#include <stdlib.h>

//日志显示内容
const std::string log_column =
    "begin-offset,end-offset,gtid,epoch,transaction-seq-no,binlog_file_name,position,"
    "next_binlog_file_name,next_position,event_size,event_type,binlog_file_"
    "name,position,next_position,";

CsvLog &CsvLog::Instance() {
  static CsvLog m_log_;
  return m_log_;
}

void CsvLog::InitLogFile() {
  char path[100];
  time_t timep;
  struct tm *p;
  time(&timep);
  p = localtime(&timep);
  sprintf(path, "fulltopiclog.%d%02d%02d.%02d%02d%02d.csv",
          (1900 + p->tm_year), (1 + p->tm_mon), p->tm_mday, p->tm_hour, p->tm_min,
          p->tm_sec);

  m_file_.open(path, std::fstream::out | std::fstream::app);
  if (!m_file_.is_open()) {
    std::cout << "open log file failed， errno:" << errno << std::endl;
    exit(-1);
  }
  m_file_ << log_column << std::endl;
}

CsvLog::CsvLog() { InitLogFile(); }

CsvLog::~CsvLog() {
  if (m_file_.is_open()) {
    m_file_.close();
    log_num_ = 0;
  }
}

std::fstream &CsvLog::GetFileFp() {
  if (!m_file_.is_open()) {
    InitLogFile();
  }

  log_num_++;
  if (log_num_ >= 100000001) {
    m_file_.flush();
    m_file_.close();
    log_num_ = 0;
  }

  if (!m_file_.is_open()) {
    InitLogFile();
  }

  return m_file_;
}

void PrintToCsv(PackingMsg *message) {
  static int message_num = 0;
  const ::rdp::messages::Transaction *trans = message->transaction;
  LOG_CSV << message->begin_offset << "," << message->end_offset << ",";
  LOG_CSV << trans->gtid() << "," << message->epoch << "," << trans->seq()
          << "," << trans->binlog_file_name() << "," << trans->position() << ","
          << trans->next_binlog_file_name() << "," << trans->next_position()
          << ",";
  int event_list_size = trans->events_size();
  LOG_CSV << event_list_size << ",";
  for (int i = 0; i < event_list_size; ++i) {
    const ::rdp::messages::Event &one_event = trans->events(i);
    LOG_CSV << one_event.event_type() << "," << one_event.binlog_file_name()
            << "," << one_event.position() << "," << one_event.next_position()
            << ",";
  }
  LOG_CSV << std::endl;
  message_num++;
  LOG_CSV.flush();
  // 文件过大，保存现有数据，并新建一个文件
  if (message_num >= 500001) {
    LOG_CSV.close();
    message_num = 0;
  }
}

