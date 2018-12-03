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

#include "syncer_events.h"
#include "syncer_filter.h"
#include "base64.h"
#include "binary_log_funcs.h"
#include "my_bit.h"
#include "my_stacktrace.h"  // my_safe_print_system_time
#include "my_time.h"
#include "mysql.h"
#include "mysql_time.h"
#include "mysql_gtidmgr.h"
#include "syncer_app.h"
#include <rdp-comm/logger.h>

namespace syncer {

// Table map event
uint64_t TablemapEvent::GetTableid(char *buf, uint32_t pos, uint8_t head_len) {
  uint64_t table_id = 0;
  uint8_t post_head_len = head_len;

  if (post_head_len == 6) {
    table_id = uint4korr(buf + pos);
    pos += 4;
  } else {
    table_id = uint6korr(buf + pos);
    pos += 6;
  }

  return table_id;
}

// Refer: mysql-5.7.17/libbinlogevents/src/rows_event.cpp
// Table_map_event::Table_map_event
int TablemapEvent::Parse() {
  //  uint32_t pos = kBinlogHeaderSize;

  Table_map_log_event *tablemap_event = new Table_map_log_event((const char *)orig_buf_, (uint)(orig_buf_len_), fd_log_event_);
  assert(tablemap_event != NULL);

  table_id_     = tablemap_event->get_table_id().id();
  column_count_ = tablemap_event->m_colcnt;
  uint64_t null_bitmap_length = (column_count_ + 7) / 8;
  null_bit_.Assign((char *)tablemap_event->m_null_bits, null_bitmap_length) ;
  
  tablemap_->set_table(table_id_, tablemap_event);

  return 0;
}

int TablemapEvent::Encode(rdp::messages::Event *event) {
  Table_map_log_event *tablemap_event = tablemap_->get_table(table_id_);
  assert(tablemap_event != NULL);

  event->set_database_name(tablemap_event->get_db_name());
  event->set_table_name(tablemap_event->get_table_name());
  event->set_event_type(cmd_);

  event->set_timestamp(pre_header_.timestamp);
  event->set_timestamp_of_receipt((uint64_t)time(NULL));
  event->set_ddl(rdp::messages::kDDLNoChage);
  event->set_position(pre_header_.log_pos - pre_header_.event_size);
  event->set_next_position(pre_header_.log_pos);
  event->set_binlog_file_name(file_name_);
  event->set_server_id(pre_header_.server_id);

  if (with_orig_buf_) event->set_orig_buff(orig_buf_, orig_buf_len_); 
  return 0;
}

// RowEvent
uint8_t RowEvent::GetVer(uint8_t cmd) {
  if (cmd == 0x17 || cmd == 0x18 || cmd == 0x19)
    return 1;
  else if (cmd == 0x1E || cmd == 0x1F || cmd == 0x20)
    return 2;

  // never go here
  return 0;
}

uint64_t RowEvent::GetTableid(char *buf, uint32_t pos, uint8_t cmd,
                              uint8_t head_len) {
  uint64_t table_id = 0;
  uint8_t post_head_len = head_len;

  if (post_head_len == 6) {
    table_id = uint4korr(buf + pos);
    pos += 4;
  } else {
    table_id = uint6korr(buf + pos);
    pos += 6;
  }

  return table_id;
}

// Refer: mysql-5.7.17/libbinlogevents/src/rows_event.cpp Rows_event::Rows_event
int RowEvent::Parse() {

  uint32_t pos = kBinlogHeaderSize;

  // Post header
  uint8_t const post_header_len = fd_log_event_->post_header_len[cmd_ - 1];
  if (post_header_len == 6) {
    table_id_ = uint4korr(orig_buf_ + pos);
    pos += 4;
  } else {
    table_id_ = uint6korr(orig_buf_ + pos);
    pos += 6;
  }

  // Flags 2 bytes
  flags_ = uint2korr(orig_buf_ + pos);
  pos += 2;

  // It is truncated, if pos >= orig_buf_len_. see TRUNC_ROWS_EVENT_SIZE.
  if (pos + 2 >= orig_buf_len_) return 0;

  // Rows, one or many rows
  Table_map_log_event *tablemap_event = tablemap_->get_table(table_id_);
  assert(tablemap_event != NULL);

  if (g_syncer_app->syncer_filter_->IsIgnored(tablemap_event->get_db_name(), 
                                              tablemap_event->get_table_name())) { 
    RDP_LOG_DBG << "Parse Ignored:" << tablemap_event->get_db_name() << tablemap_event->get_table_name();
    return 0;
  }

  TableInfo table_info;
  int32_t ret = g_syncer_app->schema_manager_->GetTableInfo(tablemap_event->get_db_name(),
                                                            tablemap_event->get_table_name(),
                                                            table_info);
  if (ret != 0)
  {
    return ret;
  }
  ColumnMap column_map = table_info.column_map_;

  // Has extra data
  if (GetVer(cmd_) == 2) {
    extra_data_length_ = uint2korr(orig_buf_ + pos);
    pos += 2;
    uint16_t real_extra_data_length = extra_data_length_ - 2;
    if (real_extra_data_length > 0) {
      extra_data_.assign(orig_buf_ + pos, real_extra_data_length);
      pos += real_extra_data_length;
    }
  }

  // Payload

  // Represents the number of columns in the table, "Packed integer"
  UnpackLenencInt(orig_buf_ + pos, pos, column_count_);

  // Indicates whether each column is used, one bit per column.
  // Bitfield 标示哪些列（字段）使用到了，也就是有数据
  uint32_t present_bitmap1_len = (column_count_ + 7) / 8;
  present_bitmap1_.Assign(orig_buf_ + pos, present_bitmap1_len);
  pos += present_bitmap1_len;

  // columns_before_image
  uint32_t uPresentBitmapBitset1 =
      present_bitmap1_.GetBitsetCount(column_count_);
  uint32_t null_bitmap_size = (uPresentBitmapBitset1 + 7) / 8;

  // columns_after_image, (for UPDATE_ROWS_EVENT only)
  uint32_t null_bitmap2_size = 0;
  if (cmd_ == 0x18 || cmd_ == 0x1F) {  // Update has previous mirror
    present_bitmap2_.Assign(orig_buf_ + pos, present_bitmap1_len);
    pos += present_bitmap1_len;

    uint32_t present_bitmap1_set2 =
        present_bitmap2_.GetBitsetCount(column_count_);
    null_bitmap2_size = (present_bitmap1_set2 + 7) / 8;
  }

  table_def *td = tablemap_event->create_table_def();
  while (orig_buf_len_ > pos) { 
    Row row;

    if (cmd_ == 0x17 || cmd_ == 0x1E) { // Insert
      row.Init(INSERT, tablemap_event->get_db_name(), tablemap_event->get_table_name());
      ParseRow(orig_buf_, pos, null_bitmap_size, row, &present_bitmap1_, td, column_map, false);
    } else if (cmd_ == 0x19 || cmd_ == 0x20) { // Delete
      row.Init(DEL, tablemap_event->get_db_name(), tablemap_event->get_table_name());
      ParseRow(orig_buf_, pos, null_bitmap_size, row, &present_bitmap1_, td, column_map, true);
    } else if (cmd_ == 0x18 || cmd_ == 0x1F) { // Update
      row.Init(UPDATE, tablemap_event->get_db_name(), tablemap_event->get_table_name());
      ParseRow(orig_buf_, pos, null_bitmap_size, row, &present_bitmap1_, td, column_map, true);
      ParseRow(orig_buf_, pos, null_bitmap2_size, row, &present_bitmap2_, td, column_map, false);
    } else {
      RDP_LOG_ERROR << "Unknown commond:" << cmd_;
      delete td;
      return -1;
    }

    //row.Print();
    rows_.push_back(row);

  }  // While

  delete td;

  return 0;
}

int RowEvent::ParseRow(char *buf, uint32_t &pos, uint32_t null_bitmap_size,
                       Row &row, MemBlock *present_bitmap, table_def *td,
                       ColumnMap &column_map, bool old) {
  int nRet = 0;

  MemBlock *null_bitmap = (old ? &row.old_null_bitmap_: &row.null_bitmap_);
  null_bitmap->Assign(buf + pos, null_bitmap_size);
  pos += null_bitmap_size;

  uint32_t uPresentPos = 0;
  for (uint32_t i = 0; i < column_count_; i++) {
    if (present_bitmap->GetBit(i)) {
      bool bNullCol = null_bitmap->GetBit(uPresentPos);
      if (bNullCol) {
        row.PushBack(i, string(""), old);
      } else {
        uint8_t col_type = td->type(i);
        uint16_t meta = td->field_metadata(i);
        bool is_unsigned = column_map[i + 1].is_unsigned_;
        nRet = ParseColumnValue(buf + pos, pos, col_type, meta, old,
                                is_unsigned, i, row);
        if (nRet == -1) return -1;
      }

      uPresentPos++;
    }
  }

  return 0;
}

int RowEvent::ParseColumnValue(char *buf, uint32_t &pos, uint8_t col_type,
                               uint16_t meta, bool old, bool is_unsigned,
                               uint32_t column_idx, Row &row) {
  uint32_t length = 0;
  // 由于mysql的历史原因，SET,ENUM,STRING 的coltype 都是
  // MYSQL_TYPE_STRING，需要用meta去进行进一步确认具体的类型
  if (col_type == MYSQL_TYPE_STRING) {
    if (meta >= 256) {
      uint8_t byte0 = meta >> 8;
      uint8_t byte1 = meta & 0xFF;
      if ((byte0 & 0x30) != 0x30) {
        // a long CHAR() field: see #37426
        // https://bugs.mysql.com/bug.php?id=37426
        length = byte1 | (((byte0 & 0x30) ^ 0x30) << 4);
        col_type = byte0 | 0x30;
      } else {
        length = meta & 0xFF;
      }
    } else {
      length = meta;
    }
  }

  char value[512] = {0};
  switch (col_type) {
    case MYSQL_TYPE_LONG: {  // int32
      if (is_unsigned) {
        uint32_t i = uint4korr(buf);
        snprintf(value, sizeof(value), "%u", i);
      } else {
        int32_t i = sint4korr(buf);
        snprintf(value, sizeof(value), "%d", i);
      }
      pos += 4;
      row.PushBack(column_idx, value, old);
      break;
    }

    case MYSQL_TYPE_TINY: {
      if (is_unsigned) {
        uint8_t c = (uint8_t)(*buf);
        snprintf(value, sizeof(value), "%hhu", c);
      } else {
        int8_t c = (int8_t)(*buf);
        snprintf(value, sizeof(value), "%hhd", c);
      }
      pos += 1;
      row.PushBack(column_idx, value, old);
      break;
    }

    case MYSQL_TYPE_SHORT: {
      if (is_unsigned) {
        uint16_t s = uint2korr(buf);
        snprintf(value, sizeof(value), "%hu", s);
      } else {
        int16_t s = sint2korr(buf);
        snprintf(value, sizeof(value), "%hd", s);
      }
      pos += 2;
      row.PushBack(column_idx, value, old);
      break;
    }

    case MYSQL_TYPE_LONGLONG: {
      if (is_unsigned) {
        uint64_t ll = uint8korr(buf);
        snprintf(value, sizeof(value), "%lu", ll);
      } else {
        int64_t ll = sint8korr(buf);
        snprintf(value, sizeof(value), "%ld", ll);
      }
      pos += 8;
      row.PushBack(column_idx, value, old);
      break;
    }

    case MYSQL_TYPE_INT24: {
      int32_t i;
      if (is_unsigned) {
        i = uint3korr(buf);
      } else {
        i = sint3korr(buf);
      }
      snprintf(value, sizeof(value), "%d", i);
      pos += 3;
      row.PushBack(column_idx, value, old);
      break;
    }

    case MYSQL_TYPE_TIMESTAMP: {
      uint32_t i = uint4korr(buf);
      snprintf(value, sizeof(value), "%u", i);
      pos += 4;
      row.PushBack(column_idx, value, old);
      break;
    }

    case MYSQL_TYPE_DATETIME: {
      uint64_t ll = uint8korr(buf);
      uint32_t d = ll / 1000000;
      uint32_t t = ll % 1000000;
      snprintf(value, sizeof(value), "%04d-%02d-%02d %02d:%02d:%02d", d / 10000,
               (d % 10000) / 100, d % 100, t / 10000, (t % 10000) / 100,
               t % 100);
      pos += 8;
      row.PushBack(column_idx, value, old);
      break;
    }

    case MYSQL_TYPE_TIME: {
      uint32_t i32 = uint3korr(buf);
      snprintf(value, sizeof(value), "%02d:%02d:%02d", i32 / 10000,
               (i32 % 10000) / 100, i32 % 100);
      pos += 3;
      row.PushBack(column_idx, value, old);
      break;
    }

    case MYSQL_TYPE_NEWDATE: {
      uint32_t tmp = uint3korr(buf);
      int part;
      char sbuf[11];
      char *spos = &sbuf[10];  // start from '\0' to the beginning

      /* Copied from field.cc */
      *spos-- = 0;  // End NULL
      part = (int)(tmp & 31);
      *spos-- = (char)('0' + part % 10);
      *spos-- = (char)('0' + part / 10);
      *spos-- = ':';
      part = (int)(tmp >> 5 & 15);
      *spos-- = (char)('0' + part % 10);
      *spos-- = (char)('0' + part / 10);
      *spos-- = ':';
      part = (int)(tmp >> 9);
      *spos-- = (char)('0' + part % 10);
      part /= 10;
      *spos-- = (char)('0' + part % 10);
      part /= 10;
      *spos-- = (char)('0' + part % 10);
      part /= 10;
      *spos = (char)('0' + part);

      snprintf(value, sizeof(value), "%s", sbuf);
      pos += 3;
      row.PushBack(column_idx, value, old);
      break;
    }

    case MYSQL_TYPE_DATE: {
      uint32_t i32 = uint3korr(buf);
      snprintf(value, sizeof(value), "%04d-%02d-%02d",
               (int32_t)(i32 / (16L * 32L)), (int32_t)(i32 / 32L % 16L),
               (int32_t)(i32 % 32L));
      pos += 3;
      row.PushBack(column_idx, value, old);
      break;
    }

    case MYSQL_TYPE_YEAR: {
      uint32_t i = (uint32_t)(uint8_t) * buf;
      snprintf(value, sizeof(value), "%04d", i + 1900);
      pos += 1;
      row.PushBack(column_idx, value, old);
      break;
    }

    case MYSQL_TYPE_ENUM: {
      switch (meta & 0xFF) {
        case 1: {
          snprintf(value, sizeof(value), "%d", (int32_t) * buf);
          pos += 1;
          row.PushBack(column_idx, value, old);
          break;
        }

        case 2: {
          int32_t i32 = uint2korr(buf);
          snprintf(value, sizeof(value), "%d", i32);
          pos += 2;
          row.PushBack(column_idx, value, old);
          break;
        }

        default:
          RDP_LOG_ERROR << "Unknown enum packlen=" << (meta & 0xFF);
          return -1;
      }
      break;
    }

    case MYSQL_TYPE_SET: {
      pos += (meta & 0xFF);
      snprintf(value, sizeof(value), "%u", uint32_t(DecodeBit((unsigned char*)buf,meta & 0xFF)));
      row.PushBack(column_idx, value, old);
      break;
    }

    case MYSQL_TYPE_BLOB: {
      switch (meta) {
        case 1: {  // TINYBLOB/TINYTEXT
          length = (uint8_t)(*buf);
          pos += length + 1;
          row.PushBack(column_idx, buf + 1, length, old);
          break;
        }

        case 2: {  // BLOB/TEXT
          length = uint2korr(buf);
          pos += length + 2;
          row.PushBack(column_idx, buf + 2, length, old);
          break;
        }

        case 3: {  // MEDIUMBLOB/MEDIUMTEXT
          length = uint3korr(buf);
          pos += length + 3;
          row.PushBack(column_idx, buf + 3, length, old);
          break;
        }

        case 4: {  // LONGBLOB/LONGTEXT
          length = uint4korr(buf);
          pos += length + 4;
          row.PushBack(column_idx, buf + 4, length, old);
          break;
        }

        default:
          RDP_LOG_ERROR << "Unknown blob type=" << meta;
          return -1;
      }
      break;
    }

    case MYSQL_TYPE_VARCHAR:
    case MYSQL_TYPE_VAR_STRING: {
      length = meta;
      if (length < 256) {
        length = (uint8_t)(*buf);
        pos += 1 + length;
        row.PushBack(column_idx, buf + 1, length, old);
      } else {
        length = uint2korr(buf);
        pos += 2 + length;
        row.PushBack(column_idx, buf + 2, length, old);
      }
      break;
    }

    case MYSQL_TYPE_STRING: {
      if (length < 256) {
        length = (uint8_t)(*buf);
        pos += 1 + length;
        row.PushBack(column_idx, buf + 1, length, old);
      } else {
        length = uint2korr(buf);
        pos += 2 + length;
        row.PushBack(column_idx, buf + 2, length, old);
      }
      break;
    }

    case MYSQL_TYPE_BIT: {
      uint32_t n_bits = (meta >> 8) * 8 + (meta & 0xFF);
      length = (n_bits + 7) / 8;
      pos += length;
      snprintf(value, sizeof(value), "%lu", DecodeBit((unsigned char*)buf,length));
      row.PushBack(column_idx, value, old);
      break;
    }

    case MYSQL_TYPE_FLOAT: {
      float fl;
      float4get(&fl, (const uchar *)buf);
      pos += 4;
      snprintf(value, sizeof(value), "%-20g", (double)fl);
      row.PushBack(column_idx, Trim(value), old);
      break;
    }

    case MYSQL_TYPE_DOUBLE: {
      double dbl;
      float8get(&dbl, buf);
      pos += 8;
      snprintf(value, sizeof(value), "%-.20g", dbl);  // Meta ?
      row.PushBack(column_idx, value, old);
      break;
    }

    case MYSQL_TYPE_NEWDECIMAL: {
      uint8_t precision = meta >> 8;
      uint8_t decimals = meta & 0xFF;
      int bin_size = my_decimal_get_binary_size(precision, decimals);
      my_decimal dec;
      binary2my_decimal(E_DEC_FATAL_ERROR, (unsigned char *)buf, &dec,
                        precision, decimals);
      int len = DECIMAL_MAX_STR_LENGTH;
      char buff[DECIMAL_MAX_STR_LENGTH + 1];
      decimal2string(&dec, buff, &len, 0, 0, 0);
      row.PushBack(column_idx, buff, old);
      pos += bin_size;
      break;
    }

    case MYSQL_TYPE_TIMESTAMP2: {  // mysql 5.6新增类型
      struct timeval tm;
      my_timestamp_from_binary(&tm, (const uchar *)buf, meta);
      uint64_t milisecond = tm.tv_sec * 1000 + tm.tv_usec / 1000;
      snprintf(value, sizeof(value), "%"PRIu64, milisecond);
      pos += my_timestamp_binary_length(meta);
      row.PushBack(column_idx, value, old);
      break;
    }

    case MYSQL_TYPE_DATETIME2: {  // mysql 5.6新增类型
      int64_t ymd, hms;
      int64_t ymdhms, ym;
      int frac;
      int64_t packed = my_datetime_packed_from_binary((const uchar *)buf, meta);
      if (packed < 0) packed = -packed;

      ymdhms = MY_PACKED_TIME_GET_INT_PART(packed);
      frac = MY_PACKED_TIME_GET_FRAC_PART(packed);

      ymd = ymdhms >> 17;
      ym = ymd >> 5;
      hms = ymdhms % (1 << 17);

      int day = ymd % (1 << 5);
      int month = ym % 13;
      int year = ym / 13;

      int second = hms % (1 << 6);
      int minute = (hms >> 6) % (1 << 6);
      int hour = (hms >> 12);

      snprintf(value, sizeof(value), "%04d-%02d-%02d %02d:%02d:%02d.%d", year,
               month, day, hour, minute, second, frac);
      pos += my_datetime_binary_length(meta);
      row.PushBack(column_idx, value, old);
      break;
    }

    case MYSQL_TYPE_TIME2: {  // mysql 5.6新增类型
      assert(meta <= DATETIME_MAX_DECIMALS);
      int64_t packed = my_time_packed_from_binary((const uchar *)buf, meta);
      if (packed < 0) packed = -packed;

      long hms = MY_PACKED_TIME_GET_INT_PART(packed);
      int frac = MY_PACKED_TIME_GET_FRAC_PART(packed);

      int hour = (hms >> 12) % (1 << 10);
      int minute = (hms >> 6) % (1 << 6);
      int second = hms % (1 << 6);
      snprintf(value, sizeof(value), "%02d:%02d:%02d.%d", hour, minute, second,
               frac);
      pos += my_time_binary_length(meta); 
      row.PushBack(column_idx, value, old);
      break;
    }

    case MYSQL_TYPE_JSON: {
      length = uint2korr(buf);
      pos += meta + length;
      row.PushBack(column_idx, buf + meta, old);
      break;
    }

    default:
      RDP_LOG_ERROR << "Don't know how to handle type=" << col_type << 
        ", meta=" << meta <<" column value";
      return -1;
  }

  return 0;
}

int RowEvent::Encode(rdp::messages::Event *event) {
  int ret = 0;

  Table_map_log_event *tablemap_event = tablemap_->get_table(table_id_);
  assert(tablemap_event != NULL);
  event->set_database_name(tablemap_event->get_db_name());
  event->set_table_name(tablemap_event->get_table_name());
  event->set_event_type(cmd_);

  TableInfo table_info;
  if (!g_syncer_app->syncer_filter_->IsIgnored(tablemap_event->get_db_name(), tablemap_event->get_table_name()) &&
      !IsSystemSchema(tablemap_event->get_db_name())) {
    ret = g_syncer_app->schema_manager_->GetTableInfo(tablemap_event->get_db_name(),
                                                      tablemap_event->get_table_name(),
                                                      table_info);
    if (ret != 0)
    {
      return ret;
    }
  } else {
    // system tables
  }

  event->set_schema_id(table_info.schema_id_);
  event->set_timestamp(pre_header_.timestamp);
  event->set_timestamp_of_receipt((uint64_t)time(NULL));
  event->set_ddl(rdp::messages::kDDLNoChage);
  event->set_position(pre_header_.log_pos - pre_header_.event_size);
  event->set_next_position(pre_header_.log_pos);
  event->set_binlog_file_name(file_name_);
  event->set_server_id(pre_header_.server_id);
  // TODO event->set_sql_statement()

  // rows
  if (!g_syncer_app->syncer_filter_->IsIgnored(tablemap_event->get_db_name(), 
                                              tablemap_event->get_table_name())) { 
    std::vector<Row> &rows = rows_;
    for (uint32_t i = 0; i < rows.size(); ++i) {
      rdp::messages::Row *row = event->add_rows();
      EncodeRow(&table_info, &rows[i], row);
    }
  } else {
    RDP_LOG_DBG << "Encode Ignored:" << tablemap_event->get_db_name() << tablemap_event->get_table_name();
  }

  if (with_orig_buf_) event->set_orig_buff(orig_buf_, orig_buf_len_); 
  return 0;
}

int RowEvent::EncodeRow(const TableInfo *table_info, Row *row_data,
                        rdp::messages::Row *row_pkg) {
  ColumnMap column_map = table_info->column_map_;
  std::map<uint32_t, std::string> &column_value = row_data->column_;
  Table_map_log_event *tablemap_event = tablemap_->get_table(table_id_); 
  table_def *td = tablemap_event->create_table_def();
  if (td == NULL) abort();
  uint32_t uPresentPos = 0;
  for (std::map<uint32_t, std::string>::iterator it = column_value.begin();
       it != column_value.end(); it++, uPresentPos++) {

    if (g_syncer_app->syncer_filter_->IsIgnored(table_info->db_name_,
                                                table_info->table_name_,
                                                column_map[it->first + 1].name_)) { 
      continue;
    }

    rdp::messages::Column *after= row_pkg->add_after();

    if (column_map.find(it->first + 1) != column_map.end()) {
      after->set_name(column_map[it->first + 1].name_);
      after->set_type(column_map[it->first + 1].type_);
      after->set_key(column_map[it->first + 1].key_);
    }
    after->set_value(it->second);

    after->set_binlog_type(td->type(it->first));
    after->set_is_null(row_data->null_bitmap_.GetBit(uPresentPos));
  }

  std::map<uint32_t, std::string> &column_value_old = row_data->old_column_;
  uPresentPos = 0;
  for (std::map<uint32_t, std::string>::iterator it = column_value_old.begin();
       it != column_value_old.end(); it++, uPresentPos++) {
    if (g_syncer_app->syncer_filter_->IsIgnored(table_info->db_name_,
                                                table_info->table_name_,
                                                column_map[it->first + 1].name_)) { 
      continue;
    }


    rdp::messages::Column *before = row_pkg->add_before();
    if (column_map.find(it->first + 1) != column_map.end()) {
      before->set_name(column_map[it->first + 1].name_);
      before->set_type(column_map[it->first + 1].type_);
      before->set_key(column_map[it->first + 1].key_);
    }
    before->set_value(it->second);

    before->set_binlog_type(td->type(it->first));
    before->set_is_null(row_data->old_null_bitmap_.GetBit(uPresentPos));
  }

  if(td) delete td;
  return 0;
}

using namespace binary_log;
// GtidEvent
/*
  The layout of the buffer is as follows:
  +------+--------+-------+-------+--------------+---------------+
  |unused|SID     |GNO    |lt_type|last_committed|sequence_number|
  |1 byte|16 bytes|8 bytes|1 byte |8 bytes       |8 bytes        |
  +------+--------+-------+-------+--------------+---------------+

  The 'unused' field is not used.

  lt_type (for logical timestamp typecode) is always equal to the
  constant LOGICAL_TIMESTAMP_TYPECODE.

  5.6 did not have TS_TYPE and the following fields. 5.7.4 and
  earlier had a different value for TS_TYPE and a shorter length for
  the following fields. Both these cases are accepted and ignored.

  The buffer is advanced in Binary_log_event constructor to point to
  beginning of post-header
*/
int GtidEvent::Parse() {
  char *ptr = orig_buf_ + kBinlogHeaderSize;
  ptr += 1;

  Uuid uuid;
  uuid.copy_from((const unsigned char *)ptr);
  ptr += 16;

  rpl_gno gno = *(rpl_gno *)ptr;
  ptr += 8;

  char uuid_str[Uuid::TEXT_LENGTH + 1] = {0x0};
  uuid.to_string(uuid_str);
  RDP_LOG_DBG << "Gtid event: " << uuid_str << ":" << gno;

  MySQLGtidMgr mgr;
  if (!mgr.AddGtid(uuid, gno)) {
    RDP_LOG_ERROR << "GtidEvent Parse Add Gtid To Mgr Failt";
    return -1;
  }
  gtid_ = mgr.ToString();

  /*
     Fetch the logical clocks. Check the length before reading, to
     avoid out of buffer reads.
  */
  if (ptr+1+16 <= orig_buf_+orig_buf_len_ && 
      *ptr == 2)
  {
    ptr += 1;
    last_committed_ = (int64_t)uint8korr(ptr);
    sequence_number_ = (int64_t)uint8korr(ptr+8);
    ptr += 16;
  }

  return 0;
}

int GtidEvent::Encode(rdp::messages::Event *event) {

  //event->set_database_name(tablemap_event->databasename_);
  //event->set_table_name(tablemap_event->tablename_);
  event->set_event_type(cmd_);

  // event->set_schema_id(table_info.schema_id_);
  event->set_timestamp(pre_header_.timestamp);
  event->set_timestamp_of_receipt((uint64_t)time(NULL));
  event->set_ddl(rdp::messages::kDDLNoChage);
  event->set_position(pre_header_.log_pos - pre_header_.event_size);
  event->set_next_position(pre_header_.log_pos);
  event->set_binlog_file_name(file_name_);
  event->set_server_id(pre_header_.server_id);

  if (with_orig_buf_) event->set_orig_buff(orig_buf_, orig_buf_len_); 
  return 0;
}

// XidEvent
int XidEvent::Parse() {
  char *ptr = orig_buf_ + kBinlogHeaderSize;
  uint64_t xid = *(uint64_t *)ptr;
  RDP_LOG_DBG << "Xid event: " <<xid;
  return 0;
}

int XidEvent::Encode(rdp::messages::Event *event) {

  //event->set_database_name(tablemap_event->databasename_);
  //event->set_table_name(tablemap_event->tablename_);
  event->set_event_type(cmd_);

  // event->set_schema_id(table_info.schema_id_);
  event->set_timestamp(pre_header_.timestamp);
  event->set_timestamp_of_receipt((uint64_t)time(NULL));
  event->set_ddl(rdp::messages::kDDLNoChage);
  event->set_position(pre_header_.log_pos - pre_header_.event_size);
  event->set_next_position(pre_header_.log_pos);
  event->set_binlog_file_name(file_name_);
  event->set_server_id(pre_header_.server_id);

  if (with_orig_buf_) event->set_orig_buff(orig_buf_, orig_buf_len_); 
  return 0;
}

// QueryEvent
int QueryEvent::Parse() {
  size_t start_offset = kBinlogHeaderSize;

  // Post-header
  start_offset += 4;  // thread_id
  start_offset += 4;  // query_exec_time
  /* Get the query to let us check for BEGIN/COMMIT/ROLLBACK */
  uint8_t schema_length = *(uint8_t *)(orig_buf_ + start_offset);
  start_offset += 1;
  // uint16_t error_code = *(uint16_t *)(buf + start_offset);
  start_offset += 2;
  uint16_t status_vars_length = *(uint16_t *)(orig_buf_ + start_offset);
  start_offset += 2;

  // Body for Query_event
  start_offset += status_vars_length;
  db_ = (char *)(orig_buf_ + start_offset);
  start_offset += (schema_length + 1);

  query_str_ = (char *)(orig_buf_ + start_offset);
  query_len_ = orig_buf_len_ - start_offset;
  if (query_len_ == 0) {
    RDP_LOG_ERROR << "Empty query string!";
    return -1;
  }

  if (/*is_ddl*/ 0) {
    RDP_LOG_DBG << "SQL statement: "<< string(query_str_, query_len_);
  }
  return 0;
}

int QueryEvent::Encode(rdp::messages::Event *event) {

  event->set_database_name(string(db_));
  event->set_event_type(cmd_);

  // event->set_schema_id(table_info.schema_id_);
  event->set_timestamp(pre_header_.timestamp);
  event->set_timestamp_of_receipt((uint64_t)time(NULL));
  if (is_ddl_) {
    event->set_ddl(rdp::messages::kDDLChanged);
  } else {
    event->set_ddl(rdp::messages::kDDLNoChage);
  }
  event->set_position(pre_header_.log_pos - pre_header_.event_size);
  event->set_next_position(pre_header_.log_pos);
  event->set_binlog_file_name(file_name_);
  event->set_server_id(pre_header_.server_id);
  event->set_sql_statement(query_str_, query_len_);

  if (with_orig_buf_) event->set_orig_buff(orig_buf_, orig_buf_len_); 
  return 0;
}

// RotateEvent
int RotateEvent::Parse() {
  orig_event_ = new Rotate_log_event(orig_buf_, orig_buf_len_, fd_log_event_);
  assert(orig_event_ != NULL);
  assert(orig_event_->ident_len > 0);

  return 0;
}

int RotateEvent::Encode(rdp::messages::Event *event) {

  //event->set_database_name(tablemap_event->databasename_);
  //event->set_table_name(tablemap_event->tablename_);
  event->set_event_type(cmd_);

  // event->set_schema_id(table_info.schema_id_);
  event->set_timestamp(pre_header_.timestamp);
  event->set_timestamp_of_receipt((uint64_t)time(NULL));
  event->set_ddl(rdp::messages::kDDLNoChage);
  event->set_position(pre_header_.log_pos - pre_header_.event_size);
  event->set_next_position(pre_header_.log_pos);
  event->set_binlog_file_name(file_name_);
  event->set_server_id(pre_header_.server_id);
  //event->set_sql_statement(orig_event_->query, orig_event_->q_len);

  if (with_orig_buf_) event->set_orig_buff(orig_buf_, orig_buf_len_); 
  return 0;
}


int OtherEvent::Encode(rdp::messages::Event *event) {

  //event->set_database_name(tablemap_event->databasename_);
  //event->set_table_name(tablemap_event->tablename_);
  event->set_event_type(cmd_);

  // event->set_schema_id(table_info.schema_id_);
  event->set_timestamp(pre_header_.timestamp);
  event->set_timestamp_of_receipt((uint64_t)time(NULL));
  event->set_ddl(rdp::messages::kDDLNoChage);
  event->set_position(pre_header_.log_pos - pre_header_.event_size);
  event->set_next_position(pre_header_.log_pos);
  event->set_binlog_file_name(file_name_);
  event->set_server_id(pre_header_.server_id);
  // event->set_sql_statement(query_str_, query_str_len_);

  if (with_orig_buf_) event->set_orig_buff(orig_buf_, orig_buf_len_); 
  return 0;
}

}  // namespace syncer
