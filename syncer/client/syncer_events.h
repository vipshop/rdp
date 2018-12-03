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

#ifndef _SYNCER_EVENT_H_
#define _SYNCER_EVENT_H_

#define __STDC_FORMAT_MACROS                                                   
#include <inttypes.h>

#include "mem_block.h"
#include "row_record.h"
#include "syncer_incl.h"
#include "syncer_main.h"
#include "my_atomic.h"         // my_atomic_add32
#include "my_bitmap.h"         // MY_BITMAP
#include "prealloced_array.h"  // Prealloced_array
#include "sql_string.h"
#include "../sql/sql_const.h"
#include "../sql/my_decimal.h"
#include "rdp.pb.h"
#include "schema_manager.h"

#ifdef __cplusplus
extern "C" {
#endif
#include "decimal.h"
#ifdef __cplusplus
}
#endif

using std::string;
using std::vector;
using std::map;

namespace syncer {

class TablemapEvent;
class RowEvent;
class GtidEvent;
class XidEvent;
class QueryEvent;

class EventBase {
public:
  explicit EventBase(uint8_t type) { cmd_ = type; } ;
  virtual ~EventBase(void) {};

  void     Init(char *buf, uint32_t buf_len, const char *file_name, 
                Format_description_log_event *fd_log_event, table_mapping *tablemap, 
                bool with_orig_buf) { 

    file_name_       = file_name;
    pre_header_      = *((PreHeader *)buf);
    fd_log_event_    = fd_log_event;
    tablemap_        = tablemap; 
    orig_buf_        = buf;
    orig_buf_len_    = buf_len;
    with_orig_buf_   = with_orig_buf; 

  };
  virtual int Parse() { return 0; };
  virtual int Encode(rdp::messages::Event *event) { return 0; };

public:
  uint8_t                       cmd_;
  string                        file_name_;
  PreHeader                     pre_header_;
  Format_description_log_event *fd_log_event_;
  table_mapping                *tablemap_;
  char                         *orig_buf_; // point to the original event buffer
  uint32_t                      orig_buf_len_;
  bool                          with_orig_buf_;
};

class TablemapEvent : public EventBase {
public:
  explicit        TablemapEvent(uint8_t type) : EventBase(type) {} ;
  virtual        ~TablemapEvent(void){};

  virtual int     Parse();
  virtual int     Encode(rdp::messages::Event *event);

  static uint64_t GetTableid(char *buf, uint32_t pos, uint8_t head_len);

public:
  uint64_t table_id_;
  uint64_t column_count_;
  MemBlock null_bit_;
  // 解析后的meta存入这里
  vector<uint16_t> meta_;
};

class RowEvent : public EventBase {
public:
  explicit        RowEvent(uint8_t type) : EventBase(type) {} ;
  virtual        ~RowEvent(){};

  virtual int     Parse();
  virtual int     Encode(rdp::messages::Event *event) ;

private:
  int             ParseRow(char *buf, uint32_t &pos, uint32_t null_bitmap_size, Row &row,
                           MemBlock *present_bitmap, table_def *td, ColumnMap &column_map, bool old);
  int             ParseColumnValue(char *buf, uint32_t &pos, uint8_t col_type,
                                   uint16_t meta, bool old, bool is_unsigned, 
                                   uint32_t column_idx, Row &row);

  static uint64_t GetTableid(char *buf, uint32_t pos, uint8_t cmd,
                             uint8_t head_len);
  int             EncodeRow(const TableInfo *table_info, Row *row_data, rdp::messages::Row *row_pkg);
  uint8_t         GetVer(uint8_t cmd) ;

public:
  uint64_t         table_id_;
  uint16_t         flags_;
  uint16_t         extra_data_length_;
  string           extra_data_;
  uint64_t         column_count_;
  MemBlock         present_bitmap1_;
  MemBlock         present_bitmap2_;
  std::vector<Row> rows_;
};

class GtidEvent : public EventBase {
public:
  explicit    GtidEvent(uint8_t type) : EventBase(type),last_committed_(0),sequence_number_(0) {} ;
  virtual    ~GtidEvent(){};

  virtual int Parse();
  virtual int Encode(rdp::messages::Event *event); 

public:
  string gtid_;
  int64_t last_committed_;
  int64_t sequence_number_;
};

class XidEvent : public EventBase {
public:
  explicit    XidEvent(uint8_t type) : EventBase(type) {} ;
  virtual    ~XidEvent(){};

  virtual int Parse();
  virtual int Encode(rdp::messages::Event *event); 
};

class RotateEvent : public EventBase {
public:
  explicit    RotateEvent(uint8_t type) : EventBase(type), orig_event_(NULL) {} ;
  virtual    ~RotateEvent(){ if (orig_event_) { delete orig_event_; }};

  virtual int Parse();
  virtual int Encode(rdp::messages::Event *event);

public:
  Rotate_log_event *orig_event_;
};

class QueryEvent : public EventBase {
public:
  explicit    QueryEvent(uint8_t type): EventBase(type), query_str_(NULL), query_len_(0), is_ddl_(false) {};
  explicit    QueryEvent(uint8_t type, bool is_ddl): EventBase(type), query_str_(NULL), query_len_(0), is_ddl_(is_ddl) {};
  virtual    ~QueryEvent(){};

  virtual int Parse();
  virtual int Encode(rdp::messages::Event *event);

public:
  char   *db_; // The default db when executing this query
  char   *query_str_;
  size_t  query_len_;
  bool is_ddl_;
};

class OtherEvent : public EventBase {
public:
  explicit    OtherEvent(uint8_t type) : EventBase(type) {} ;
  virtual    ~OtherEvent(){};

  virtual int Parse() { return 0; };
  virtual int Encode(rdp::messages::Event *event); 

public:
};

} // namespace syncer

#endif // _SYNCER_EVENT_H_
