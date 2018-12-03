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

#include "binlog_writer.h"

namespace syncer {

const uint32_t default_os_page_size_ = 4096;

// Constructor
PosixWriter::PosixWriter(FileSyncMethod sync_method)
    : BinlogWriter(), fd_(NULL), last_sync_pos_(0), current_write_offset_(0),
      last_flush_pos_(0), sync_method_(sync_method) {}

// Destructor
PosixWriter::~PosixWriter(void) { this->Close(); }

// Write a block of data, caller can specify whether
// to sync pending page cache to disk. Generally a sync
// should issue when we encounter a XID event which means
// the end of a transaction
bool PosixWriter::Write(const char *buf, size_t len, bool sync) {
  // Should aligning write by 4K multiples, such that 'READ MODIFY WRITE'
  // can be avoided, usually there are 3 ways to avoid RMW:
  // 1. append write, but requires sync meta info
  // 2. write page in cache, requires much memory
  // 3. aligning write by system page size
  if (1 != fwrite_unlocked(buf, len, 1, fd_)) {
    RDP_LOG_ERROR << "fwrite failed: " << GetLastErrorMsg(errno).c_str();
    return false;
  }

  current_write_offset_ += len;
  if (sync)
    return this->Sync();
  else
    return true;
}

// Write a block of data, caller can specify whether
// to sync pending page cache to disk. Generally a sync
// should issue when we encounter a XID event which means
// the end of a transaction
bool PosixWriter::Write(const char *log_file, const char *buf, size_t len,
                        bool sync) {
  if (NULL == fd_) {
    if (!this->Create(log_file)) {
      return false;
    }
  }

  // Should aligning write by 4K multiples, such that 'READ MODIFY WRITE'
  // can be avoided, usually there are 3 ways to avoid RMW:
  // 1. append write, but requires sync meta info
  // 2. write page in cache, requires much memory
  // 3. aligning write by system page size
  if (1 != fwrite_unlocked(buf, len, 1, fd_)) {
    RDP_LOG_ERROR << "fwrite failed: " << GetLastErrorMsg(errno).c_str();
    return false;
  }

  current_write_offset_ += len;
  if (sync)
    return this->Sync();
  else
    return true;
}

// Close current file (if any) and create a new file by
// file_name, it's a simple combine of Close and Create
bool PosixWriter::Rotate(const char *file_name, bool truncate_file) {
  this->Close();
  return this->Create(file_name, truncate_file);
}

// Flush glibc/CRT buffer to page cache synchronously
bool PosixWriter::Flush(void) {
  // Flush from user space to system page
  if (0 != fflush_unlocked(fd_)) {
    RDP_LOG_ERROR << "flush failed: " << GetLastErrorMsg(errno).c_str();
    return false;
  }

  last_flush_pos_ = current_write_offset_;
  return true;
}

// Flush page cache to disk media # !!!asynchronously!!! #
bool PosixWriter::Sync(void) {
  // Already synced
  if (current_write_offset_ == last_sync_pos_)
    return true;

  // Flush from user space to system page
  if (0 != fflush_unlocked(fd_)) {
    RDP_LOG_ERROR << "flush failed: " << GetLastErrorMsg(errno).c_str();
    return false;
  }

  last_flush_pos_ = current_write_offset_;

  // Request to sync to disk
  int ret = 0;
  unsigned int sync_flags = SYNC_FILE_RANGE_WRITE;
  switch (sync_method_) {
  case SYNC_ASYNC:
    ret = sync_file_range(fileno_unlocked(fd_), last_sync_pos_,
                          current_write_offset_ - last_sync_pos_, sync_flags);
    break;

  case SYNC_RANGE:
    sync_flags |= SYNC_FILE_RANGE_WAIT_BEFORE;
    sync_flags |= SYNC_FILE_RANGE_WAIT_AFTER;
    ret = sync_file_range(fileno_unlocked(fd_), last_sync_pos_,
                          current_write_offset_ - last_sync_pos_, sync_flags);
    break;

  case SYNC_DATASYNC:
    ret = fdatasync(fileno_unlocked(fd_));
    break;

  case SYNC_SYNC:
    while (-1 == (ret = fsync(fileno_unlocked(fd_))) && errno == EINTR)
      ;
    break;

  default:
    RDP_LOG_ERROR << "Unknown sync method: " << sync_method_;
    return false;
  }

  if (ret < 0) {
    RDP_LOG_ERROR << "fsync failed: " << GetLastErrorMsg(errno).c_str();
    return false;
  }

  last_sync_pos_ = current_write_offset_;
  return true;
}

// Open a new file by name
// "a" Open for appending (writing at end of file).
// The file is created if it does not exist.
// The stream is positioned at the end of the file.
bool PosixWriter::Create(const char *file_name, bool truncate_file) {
  char tmp[256];
  struct stat st;

  AppConfig *conf = g_syncer_app->app_conf_;
  snprintf(tmp, sizeof(tmp), "%s/%s",
           conf->GetValue<std::string>("binlog.dir", "./binlog").c_str(),
           file_name);

  if (truncate_file) {
    // Log warning in case we truncate an exist file
    if (0 == stat(tmp, &st)) {
      if (st.st_size > 0) {
        RDP_LOG_DBG << "Truncate "<< tmp << " from offset " << st.st_size << " to 0";
        fd_ = fopen_unlocked(tmp, "wb");
        st.st_size = 0;
      } else {
        fd_ = fopen_unlocked(tmp, "ab");
      }
    } else {
      fd_ = fopen_unlocked(tmp, "ab");
      st.st_size = 0;
    }
  } else {
    fd_ = fopen_unlocked(tmp, "ab");
    stat(tmp, &st);
  }

  if (NULL == fd_) {
    RDP_LOG_ERROR << "fopen " << tmp << " failed: " << GetLastErrorMsg(errno).c_str();
    return false;
  }

  // Change this buffer mainly for 'very big transaction' cache,
  // we try to only flush when we encounter end of transaction;
  if (0 != setvbuf(fd_, NULL, _IOFBF,
                   std::max((uint32_t)st.st_blksize, default_os_page_size_))) {
    RDP_LOG_DBG << "setvbuf failed: "<< GetLastErrorMsg(errno).c_str();
  }

  fcntl(fileno_unlocked(fd_), F_SETFL, O_NOATIME);
  __fsetlocking(fd_, FSETLOCKING_BYCALLER);

  // Since logbus validates the trx boundary at startup, so we
  // think there is no partial trx when we open a binlog file.
  last_sync_pos_ = st.st_size;
  current_write_offset_ = st.st_size;
  last_flush_pos_ = st.st_size;
  return true;
}

// Close current file (if any)
bool PosixWriter::Close(void) {
  if (NULL != fd_) {
    this->Sync();
    fclose_unlocked(fd_);
    fd_ = NULL;
  }
  return true;
}

// Truncate current file to last flushed offset
bool PosixWriter::TruncateToFlushedPosition(void) {
  if (NULL != fd_ && last_flush_pos_ != current_write_offset_) {
    RDP_LOG_DBG << "Truncate active binlog from " << current_write_offset_ << 
      " to " << last_flush_pos_;
    if (0 != ftruncate64(fileno_unlocked(fd_), (long)last_flush_pos_)) {
      RDP_LOG_ERROR << "ftruncate64 to flushed position failed: " << GetLastErrorMsg(errno).c_str();
    }
    fflush_unlocked(fd_);
  }

  return true;
}

} // namespace syncer
