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

#include "file_mgt.h"

#include <sys/types.h>
#include <dirent.h>

namespace rdp_comm {

FileMgt::FileMgt(const string& dir, const string& basename,
                 const string& suffix, size_t roll_size, int days)
    : dir_(dir),
      basename_(basename),
      suffix_(suffix),
      roll_size_(roll_size),
      days_(days),
      written_bytes_(0),
      last_roll_(0),
      file_(NULL) {
  assert(basename.find('/') == string::npos);
  assert(0 == RollFile());
}

FileMgt::~FileMgt() {
  if (file_) {
    fclose(file_);
  }
}

int FileMgt::Append(const char* line, int len) {
  lock_guard guard(&lock_);
  // write file
  if (1 != fwrite(line, len, 1, file_)) {
    return -1;
  }

  written_bytes_ += len;
  // check file roll size
  if (written_bytes_ > roll_size_) {
    if (RollFile()) {
      return -1;
    }
  }
  return 0;
}

void FileMgt::Flush() { fflush(file_); }

FILE* FileMgt::GetFileHandle() { return file_; }

void FileMgt::DeleteOldFiles(time_t* now) {
  static int suffix_len = suffix_.length();

  // open log dir
  DIR* dir = opendir(dir_.c_str());
  if (dir == NULL) {
    return;
  }

  struct dirent* entry;
  while ((entry = readdir(dir))) {
    if (entry->d_type != DT_REG) {
      continue;
    }
    // get log files
    if (strncmp(entry->d_name, basename_.c_str(), basename_.size()) != 0) {
      continue;
    }

    // log suffix check
    int len = strlen(entry->d_name);
    if (strncmp(&(entry->d_name[len - suffix_len]), suffix_.c_str(), suffix_len) !=
        0) {
      continue;
    }

    string path = dir_ + '/' + entry->d_name;
    struct stat sb;
    if (lstat(path.c_str(), &sb)) {
      continue;
    }

    // check last modify time
    if (difftime(*now, sb.st_mtime) > 3600 * 24 * days_) {
      unlink(path.c_str());
    }
  }
}

int FileMgt::RollFile() {
  time_t now = 0;
  now = time(NULL);
  string new_filename = dir_ + '/' + GetFileName(&now) + suffix_;
  // curr file allways "basename_ + suffix_"
  string src_filename = dir_ + '/' + basename_ + suffix_;

  if (now > last_roll_) {
    // Delete old files
    DeleteOldFiles(&now);
    last_roll_ = now;

    if (file_) {
      fclose(file_);
      file_ = NULL;
    }

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

string FileMgt::GetFileName(time_t* now) {
  string filename;
  filename = basename_;

  char timebuf[32];
  struct tm tm;
  gmtime_r(now, &tm);
  strftime(timebuf, sizeof timebuf, ".%Y%m%d%H%M%S", &tm);
  filename += timebuf;

  return filename;
}

}  // namespace rdp_comm
