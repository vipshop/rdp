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

#ifndef _BINLOG_READER_H_
#define _BINLOG_READER_H_

#include <rdp-comm/signal_handler.h>
#include <rdp-comm/logger.h>
#include "syncer_incl.h"
#include "syncer_main.h"

namespace syncer {

// Base file operation definition
class FileOps {
 public:
  explicit FileOps() {};
  virtual ~FileOps() {};

 public:
  // Make hint to OS about the page cache behavior
  virtual void Hint(void) {};

  // Set buffering behavior of current file
  virtual void SetBuffer(void) {};
};

// Read GTIDs from a binlog file event by event, optionally skip
// other type of log events under various scenarios
class BinlogReader : public FileOps {
 public:
  // Constructor
  explicit BinlogReader(const char *fn, int64_t offset = 4);
  // Destructor
  virtual ~BinlogReader(void);

  // Wait until file header had been flushed
  inline bool WaitFlush(void) {
    if (file_size_ < cur_offset_) {
      // Checking whether requested offset is valid
      struct stat st;
      if (0 != fstat(fileno_unlocked(file_handle_), &st)) {
        RDP_LOG_ERROR << "stat "<< fileno_unlocked(file_handle_) << " failed: " << 
          GetLastErrorMsg(errno).c_str();
        assert(0);
      } else {
        file_size_ = st.st_size;
      }
    }

    return (file_size_ < cur_offset_);
  }

  // Seek to destination, skip the magic number
  bool Begin(void);

  // Fetch a log event by specific type, if no type specified
  // return any type of valid log event orderly
  // Change return code to int:
  // Return
  // READ_OK  : Succeed
  // READ_EOF : EOF
  // READ_ERR : Error
  // TODO: identify eof and error, handle partial reading!
  FileReadStatus Next(void);

  // Test end of file
  inline bool End(void) {
    if (feof_unlocked(file_handle_)) {
      clearerr_unlocked(file_handle_);
      return true;
    }
    return false;
  }

  // Last read event file in bytes
  inline int32_t LogEventSize(void) const { return event_size_; }

  // Last read event type
  inline uint8_t GetEventType(void) const { return event_type_; }

  // Payload of last read event with a reserved 5 bytes header,
  // such that we can fill mysql protocol public header
  // (3 bytes length + 1 byte Seq No.) and 1 bytes of OK-Bytes (Magic No.)
  inline uint8_t *Ptr(void) const { return BinlogReader::read_buffer_; }

  // Make hint to OS about the page cache behavior
  virtual void Hint(void);

 public:
  // Sequence No. of current binlog file
  uint32_t binlog_id_;

  // Share among all instances
  static uint8_t *read_buffer_;
  static int32_t buffer_size_;

  // Copy of last heartbeat event, if case of fake rotate to a new
  // file and have no events comes, we should send previous copy of
  // heartbeat, or else slave will report compatible
  static char last_fn_[256];
  static uint32_t cur_pos_;

 private:
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
  FileReadStatus ReadLogEvent(void);

 private:
  FILE *file_handle_;
  int64_t cur_offset_;
  int64_t file_size_;
  char file_name_[256];
  // Calculate the file name length once
  size_t file_name_len_;
  uint8_t event_type_;
  int32_t event_size_;

  enum_binlog_checksum_alg checksum_alg_;
};  // BinlogReader

}  // namespace syncer

#endif  // _BINLOG_READER_H_
