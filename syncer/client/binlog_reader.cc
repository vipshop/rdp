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

#include "binlog_reader.h"
#include "syncer_app.h"
#include "syncer_incl.h"

namespace syncer {

// Share among all instances
uint8_t *BinlogReader::read_buffer_ = nullptr;
int32_t BinlogReader::buffer_size_ = (16 << 20);

// TODO:
// init while no binlog sending had performed ?
char BinlogReader::last_fn_[256] = {0x0};
uint32_t BinlogReader::cur_pos_ = 0;

// Read GTIDs from a binlog file event by event, optionally skip
// other type of log events under various scenarios

// Constructor
BinlogReader::BinlogReader(const char *fn, int64_t offset)
    : FileOps(),
      cur_offset_(offset),
      file_size_(0),
      event_type_(0),
      event_size_(0),
      checksum_alg_(binary_log::BINLOG_CHECKSUM_ALG_OFF) {
  char tmp[256];
  AppConfig *conf = g_syncer_app->app_conf_;
  const char *p = strrchr(fn, '.');
  if (NULL != p) {
    binlog_id_ = atoi(++p);
    RDP_LOG_DBG << "Switch to binlog id: " << binlog_id_;
  }
  snprintf(tmp, sizeof(tmp), "%s/%s",
           conf->GetValue<std::string>("binlog.dir").c_str(), fn);

  // Open target file
  file_handle_ = fopen_unlocked(tmp, "rb");
  if (NULL == file_handle_) {
    RDP_LOG_ERROR << "fopen " << tmp << " failed: " << GetLastErrorMsg(errno).c_str();
    assert(0);
  }

  this->SetBuffer();
  this->Hint();

  // Allocate buffer for log events reading
  // TODO: all binlog reader instances share the same malloc buffer?
  snprintf(file_name_, sizeof(file_name_), "%s", fn);
  file_name_len_ = strlen(fn);
  if (nullptr == BinlogReader::read_buffer_) {
    BinlogReader::read_buffer_ = new uint8_t[buffer_size_];
  }
  assert(read_buffer_);
}

// Destructor
BinlogReader::~BinlogReader(void) {
  // Cache last file name
  snprintf(last_fn_, sizeof(last_fn_), "%s", file_name_);
  if (NULL != file_handle_) {
    fclose_unlocked(file_handle_);
    file_handle_ = NULL;
  }
}

// Seek to destination, skip the magic number
bool BinlogReader::Begin(void) {
  RDP_LOG_DBG << "File: " << file_name_ << "'s start offset: " << cur_offset_;

  // Last event is format description event and offset is 4U,
  // This means previous is a FDE event, so we need NOT resend
  // duplicated log event again; Move offset_ to by pass FDE event
  if (binary_log::FORMAT_DESCRIPTION_EVENT == event_type_ &&
      4U == cur_offset_) {
    cur_offset_ += event_size_;
    return true;
  }

  if (0 != fseek_unlocked(file_handle_, cur_offset_, SEEK_SET)) {
    RDP_LOG_ERROR << "fseek failed: " << GetLastErrorMsg(errno).c_str();
    return false;
  }

  return true;
}

// Fetch a log event by specific type, if no type specified
// return any type of valid log event orderly
// Change return code to int:
// Return
// READ_OK  : Succeed
// READ_EOF : EOF
// READ_ERR : Error
// TODO: identify eof and error, handle partial reading!
FileReadStatus BinlogReader::Next(void) {
  FileReadStatus rc = ReadLogEvent();
  if (READ_OK == rc) {
    // Only move offset if reading OK
    cur_offset_ += event_size_;
  }
  return rc;
}

// Read a full log event from file, only update event type & event length
// in case we have read the full event, or else reset to 0
//
// Change return code to int:
// Return
// READ_OK  : Succeed
// READ_EOF : EOF
// READ_ERR : Error
//
// Currently we assume:
// If we read 'eof' and the file it NOT the last binlog, we recognize this
// as normal case and no other checking, binlogsender will rotate to next
// binlog file.
FileReadStatus BinlogReader::ReadLogEvent(void) {
  uint8_t *ptr = read_buffer_ + 5;

  do {
    // Reading log event public header
    if (1 != fread_unlocked(ptr, kBinlogHeaderSize, 1, file_handle_)) {
      if (!feof_unlocked(file_handle_) ||
          ferror_unlocked(file_handle_)) {  // error
        RDP_LOG_ERROR << "fread failed: " << GetLastErrorMsg(errno).c_str();
        goto err_t;
      } else {  // eof
        clearerr_unlocked(file_handle_);
        if (0 != fseek_unlocked(file_handle_, cur_offset_, SEEK_SET)) {
          RDP_LOG_ERROR << "fseek failed: " << GetLastErrorMsg(errno).c_str();
          goto err_t;
        }

        goto eof_t;
      }
    }

    // Verify event type
    event_type_ = *(uint8_t *)(ptr + 4);
    event_size_ = *(int32_t *)(ptr + 9);
    assert(event_size_ >= kBinlogHeaderSize);
    if (event_type_ <= binary_log::UNKNOWN_EVENT ||
        event_type_ >= binary_log::ENUM_END_EVENT) {
      RDP_LOG_ERROR << "Illegal event type " << event_type_ << " @ "<< file_name_ << ":" << cur_offset_;
      goto err_t;
    }

    // Expand ptr_ size accordingly
    if (event_size_ + 5 > BinlogReader::buffer_size_) {
      BinlogReader::buffer_size_ = event_size_ + 5;
      uint8_t *tmp_ = new uint8_t[BinlogReader::buffer_size_];
      assert(tmp_);
      memcpy(tmp_, BinlogReader::read_buffer_, kBinlogHeaderSize + 5);
      delete[] BinlogReader::read_buffer_;
      BinlogReader::read_buffer_ = tmp_;
      ptr = BinlogReader::read_buffer_ + 5;
    }

    // Stop event has only public header
    if (event_size_ == kBinlogHeaderSize) {
      cur_pos_ = *(uint32_t *)(ptr + 13);
      return READ_OK;
    }

    // Reading log event payload
    if (1 != fread_unlocked(ptr + kBinlogHeaderSize,
                            event_size_ - kBinlogHeaderSize, 1, file_handle_)) {
      if (!feof(file_handle_) || ferror_unlocked(file_handle_)) {  // error
        RDP_LOG_ERROR << "fread failed: " << GetLastErrorMsg(errno).c_str();
        goto err_t;
      } else {  // eof
        clearerr_unlocked(file_handle_);
        if (0 != fseek_unlocked(file_handle_, cur_offset_, SEEK_SET)) {
          RDP_LOG_ERROR << "fseek failed: " << GetLastErrorMsg(errno).c_str();
          goto err_t;
        }

        goto eof_t;
      }
    }

    // Setting checksum algorithm
    if (binary_log::FORMAT_DESCRIPTION_EVENT == event_type_) {
      checksum_alg_ =
          Log_event_footer::get_checksum_alg((const char *)ptr, event_size_);
      RDP_LOG_DBG << "File " << file_name_ << "'s checksum is: " << 
              (checksum_alg_ != binary_log::BINLOG_CHECKSUM_ALG_OFF ? "CRC32" : "OFF");
    }

    cur_pos_ = *(uint32_t *)(ptr + 13);
    return READ_OK;
  } while (rdp_comm::signalhandler::IsRunning());

eof_t:
  // Encounter eof
  event_type_ = binary_log::UNKNOWN_EVENT;
  event_size_ = 0;
  return READ_EOF;

err_t:
  // Encounter some error
  event_type_ = binary_log::UNKNOWN_EVENT;
  event_size_ = 0;
  return READ_ERR;
}

// Make hint to OS about the page cache behavior
void BinlogReader::Hint(void) {
  // Checking whether requested offset is valid
  struct stat st;
  if (0 != fstat(fileno_unlocked(file_handle_), &st)) {
    RDP_LOG_ERROR << "stat " << fileno_unlocked(file_handle_) << " failed: " << GetLastErrorMsg(errno).c_str();
    assert(0);
  } else {
    file_size_ = st.st_size;
    if (st.st_size < cur_offset_) {
      // Due to only flush XID, after rotate the magic bytes is in user space
      RDP_LOG_DBG << "Required offset out of range: " << cur_offset_ << " > " << st.st_size;
      // assert(0);
    }
  }

  fcntl(fileno_unlocked(file_handle_), F_SETFL, O_NOATIME);
  __fsetlocking(file_handle_, FSETLOCKING_BYCALLER);
  int ret = posix_fadvise(fileno_unlocked(file_handle_), 0, st.st_size,
                          POSIX_FADV_SEQUENTIAL);
  if (ret < 0) {
    RDP_LOG_ERROR << "posix_fadvise " << fileno_unlocked(file_handle_) << " failed: " << GetLastErrorMsg(errno).c_str();
    assert(0);
  }
}

}  // namespace syncer
