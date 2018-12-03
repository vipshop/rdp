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

#ifndef RDP_COMM_FILE_MGT_H
#define RDP_COMM_FILE_MGT_H

#include <string>
#include "util.h"

using std::string;

namespace rdp_comm {
class FileMgt {
 public:
  FileMgt(const string& dir, const string& basename, const string& suffix, size_t roll_size,
          int days);
  ~FileMgt();

  int Append(const char* logline, int len);
  void Flush();
  FILE* GetFileHandle();

 private:
  int RollFile();
  void DeleteOldFiles(time_t* now);
  string GetFileName(time_t* now);

  const string dir_;
  const string basename_;
  const string suffix_;
  const size_t roll_size_;
  int days_;

  size_t written_bytes_;

  time_t last_roll_;
  FILE* file_;

  spinlock_t lock_;
};
}  // namespace rdp_comm
#endif
