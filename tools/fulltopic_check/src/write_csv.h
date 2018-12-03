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

#ifndef RDP_SRC_WRITE_CSV_H
#define RDP_SRC_WRITE_CSV_H

#include <stdio.h>
#include <fstream>
#include <rdp.pb.h>
#include "continuity_check.h"

#define LOG_CSV CsvLog::Instance().GetFileFp()

using std::fstream;

class CsvLog {
 public:
  ~CsvLog();
  fstream& GetFileFp();
  static CsvLog& Instance();

 private:
  explicit CsvLog();
  void InitLogFile();
  int log_num_;
  fstream m_file_;
};

void PrintToCsv(PackingMsg *packing_msg);

#endif // RDP_SRC_WRITE_CSV_H
