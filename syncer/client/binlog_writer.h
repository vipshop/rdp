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

#ifndef _BINLOG_WRITER_H_
#define _BINLOG_WRITER_H_

#include "syncer_app.h"
#include "syncer_incl.h"

namespace syncer {

class BinlogWriter {
public:
  // Constructor
  explicit BinlogWriter(void) {}

  // Destructor
  virtual ~BinlogWriter(void) {}

  // Close current file (if any) and create a new file by
  // file_name, it's a simple combine of Close and Create
  virtual bool Rotate(const char *file_name, bool truncate_file = false) = 0;

  // Write a block of data, caller can specify whether
  // to sync pending page cache to disk. Generally a sync
  // should issue when we encounter a XID event which means
  // the end of a transaction
  virtual bool Write(const char *buf, size_t len, bool sync = false) = 0;

  // Write by event entry
  // virtual bool Write(const EventEntry* entry, bool sync = false) = 0;

  // Write by event entry
  virtual bool Write(const char *log_file, const char *buf, size_t len,
                     bool sync = false) = 0;

  // Flush glibc/CRT buffer to page cache synchronously
  virtual bool Flush(void) = 0;

  // Flush page cache to disk media synchronously
  virtual bool Sync(void) = 0;

  // Close current file (if any)
  virtual bool Close(void) = 0;

  // Truncate current file to last flushed offset
  virtual bool TruncateToFlushedPosition(void) = 0;

  // Return current file offset
  virtual int64_t GetPos(void) const = 0;

private:
  // Open a new file by name
  virtual bool Create(const char *file_name, bool truncate_file = false) = 0;
};

class PosixWriter : public BinlogWriter {
public:
  // Constructor
  explicit PosixWriter(FileSyncMethod sync_method);

  // Destructor
  virtual ~PosixWriter(void);

  // Close current file (if any) and create a new file by
  // file_name, it's a simple combine of Close and Create
  virtual bool Rotate(const char *file_name, bool truncate_file = false);

  // Write a block of data, caller can specify whether
  // to sync pending page cache to disk. Generally a sync
  // should issue when we encounter a XID event which means
  // the end of a transaction
  virtual bool Write(const char *buf, size_t len, bool sync = false);

  // Write by event entry
  // virtual bool Write(const EventEntry* entry, bool sync = false);

  // Write by event entry
  virtual bool Write(const char *log_file, const char *buf, size_t len,
                     bool sync = false);

  // Flush glibc/CRT buffer to page cache synchronously
  virtual bool Flush(void);

  // Flush page cache to disk media synchronously
  virtual bool Sync(void);

  // Close current file (if any)
  virtual bool Close(void);

  // Truncate current file to last flushed offset
  virtual bool TruncateToFlushedPosition(void);

  // Return current file offset
  virtual int64_t GetPos(void) const { return current_write_offset_; }

private:
  // Open a new file by name
  virtual bool Create(const char *file_name, bool truncate_file = false);

private:
  FILE *fd_;
  off_t last_sync_pos_;
  off_t current_write_offset_;
  off_t last_flush_pos_;
  FileSyncMethod sync_method_;
};

} // namespace syncer

#endif // _BINLOG_WRITER_H_
