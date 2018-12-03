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

#include <rdp-comm/signal_handler.h>
#include <rdp-comm/logger.h>

#include "rows_log_event_ex.h"
#include "syncer_app.h"

#include "base64.h"
#include "binary_log_funcs.h"
#include "my_atomic.h"  // my_atomic_add32
#include "my_bit.h"
#include "my_bitmap.h"      // MY_BITMAP
#include "my_stacktrace.h"  // my_safe_print_system_time
#include "my_time.h"
#include "mysql.h"
#include "mysql_time.h"

using namespace std;

namespace syncer {

/**
  Prints a quoted string to io cache.
  Control characters are displayed as hex sequence, e.g. \x00

  @param[in] file              IO cache
  @param[in] prt               Pointer to string
  @param[in] length            String length
*/

static void my_b_write_quoted(const uchar *ptr, uint length) {
  const uchar *s;
  fprintf(stdout, "'");
  for (s = ptr; length > 0; s++, length--) {
    if (*s > 0x1F && *s != '\'' && *s != '\\')
      fprintf(stdout, (const char *)s, 1);
    else {
      uchar hex[10];
      size_t len = my_snprintf((char *)hex, sizeof(hex), "%s%02x", "\\x", *s);
      fprintf(stdout, (const char *)hex, len);
    }
  }
  fprintf(stdout, "'");
}

/**
  Prints a bit string to io cache in format  b'1010'.

  @param[in] file              IO cache
  @param[in] ptr               Pointer to string
  @param[in] nbits             Number of bits
*/
static void my_b_write_bit(const uchar *ptr, uint nbits) {
  uint bitnum, nbits8 = ((nbits + 7) / 8) * 8, skip_bits = nbits8 - nbits;
  fprintf(stdout, "b'");
  for (bitnum = skip_bits; bitnum < nbits8; bitnum++) {
    int is_set = (ptr[(bitnum) / 8] >> (7 - bitnum % 8)) & 0x01;
    fprintf(stdout, (const char *)(is_set ? "1" : "0"));
  }
  fprintf(stdout, "'");
}

/**
  Prints a packed string to io cache.
  The string consists of length packed to 1 or 2 bytes,
  followed by string data itself.

  @param[in] file              IO cache
  @param[in] ptr               Pointer to string
  @param[in] length            String size

  @retval   - number of bytes scanned.
*/
static size_t my_b_write_quoted_with_length(const uchar *ptr, uint length) {
  if (length < 256) {
    length = *ptr;
    my_b_write_quoted(ptr + 1, length);
    return length + 1;
  } else {
    length = uint2korr(ptr);
    my_b_write_quoted(ptr + 2, length);
    return length + 2;
  }
}

/**
  Prints a 32-bit number in both signed and unsigned representation

  @param[in] file              IO cache
  @param[in] sl                Signed number
  @param[in] ul                Unsigned number
*/
static void my_b_write_sint32_and_uint32(int32 si, uint32 ui) {
  fprintf(stdout, "%d", si);
  if (si < 0) fprintf(stdout, " (%u)", ui);
}

/**
  Print a packed value of the given SQL type into IO cache

  @param[in] file ß             IO cache
  @param[in] ptr               Pointer to string
  @param[in] type              Column type
  @param[in] meta              Column meta information
  @param[out] typestr          SQL type string buffer (for verbose output)
  @param[out] typestr_length   Size of typestr

  @retval   - number of bytes scanned from ptr.
*/
static size_t log_event_print_value(const uchar *ptr, uint type, uint meta,
                                    char *typestr, size_t typestr_length) {
  uint32 length = 0;

  if (type == MYSQL_TYPE_STRING) {
    if (meta >= 256) {
      uint byte0 = meta >> 8;
      uint byte1 = meta & 0xFF;

      if ((byte0 & 0x30) != 0x30) {
        /* a long CHAR() field: see #37426 */
        length = byte1 | (((byte0 & 0x30) ^ 0x30) << 4);
        type = byte0 | 0x30;
      } else
        length = meta & 0xFF;
    } else
      length = meta;
  }

  switch (type) {
    case MYSQL_TYPE_LONG: {
      my_snprintf(typestr, typestr_length, "INT");
      if (!ptr) return fprintf(stdout, "NULL");
      int32 si = sint4korr(ptr);
      uint32 ui = uint4korr(ptr);
      my_b_write_sint32_and_uint32(si, ui);
      return 4;
    }

    case MYSQL_TYPE_TINY: {
      my_snprintf(typestr, typestr_length, "TINYINT");
      if (!ptr) return fprintf(stdout, "NULL");
      my_b_write_sint32_and_uint32((int)(signed char)*ptr,
                                   (uint)(unsigned char)*ptr);
      return 1;
    }

    case MYSQL_TYPE_SHORT: {
      my_snprintf(typestr, typestr_length, "SHORTINT");
      if (!ptr) return fprintf(stdout, "NULL");
      int32 si = (int32)sint2korr(ptr);
      uint32 ui = (uint32)uint2korr(ptr);
      my_b_write_sint32_and_uint32(si, ui);
      return 2;
    }

    case MYSQL_TYPE_INT24: {
      my_snprintf(typestr, typestr_length, "MEDIUMINT");
      if (!ptr) return fprintf(stdout, "NULL");
      int32 si = sint3korr(ptr);
      uint32 ui = uint3korr(ptr);
      my_b_write_sint32_and_uint32(si, ui);
      return 3;
    }

    case MYSQL_TYPE_LONGLONG: {
      my_snprintf(typestr, typestr_length, "LONGINT");
      if (!ptr) return fprintf(stdout, "NULL");
      char tmp[64];
      longlong si = sint8korr(ptr);
      longlong10_to_str(si, tmp, -10);
      fprintf(stdout, "%s", tmp);
      if (si < 0) {
        ulonglong ui = uint8korr(ptr);
        longlong10_to_str((longlong)ui, tmp, 10);
        fprintf(stdout, " (%s)", tmp);
      }
      return 8;
    }

    case MYSQL_TYPE_NEWDECIMAL: {
      uint precision = meta >> 8;
      uint decimals = meta & 0xFF;
      my_snprintf(typestr, typestr_length, "DECIMAL(%d,%d)", precision,
                  decimals);
      if (!ptr) return fprintf(stdout, "NULL");
      uint bin_size = my_decimal_get_binary_size(precision, decimals);
      my_decimal dec;
      binary2my_decimal(E_DEC_FATAL_ERROR, (uchar *)ptr, &dec, precision,
                        decimals);
      int len = DECIMAL_MAX_STR_LENGTH;
      char buff[DECIMAL_MAX_STR_LENGTH + 1];
      decimal2string(&dec, buff, &len, 0, 0, 0);
      fprintf(stdout, "%s", buff);
      return bin_size;
    }

    case MYSQL_TYPE_FLOAT: {
      my_snprintf(typestr, typestr_length, "FLOAT");
      if (!ptr) return fprintf(stdout, "NULL");
      float fl;
      float4get(&fl, ptr);
      char tmp[320];
      sprintf(tmp, "%-20g", (double)fl);
      fprintf(stdout, "%s", tmp); /* my_snprintf doesn't support %-20g */
      return 4;
    }

    case MYSQL_TYPE_DOUBLE: {
      strcpy(typestr, "DOUBLE");
      if (!ptr) return fprintf(stdout, "NULL");
      double dbl;
      float8get(&dbl, ptr);
      char tmp[320];
      sprintf(tmp, "%-.20g", dbl); /* my_snprintf doesn't support %-20g */
      fprintf(stdout, "%s", tmp);
      return 8;
    }

    case MYSQL_TYPE_BIT: {
      /* Meta-data: bit_len, bytes_in_rec, 2 bytes */
      uint nbits = ((meta >> 8) * 8) + (meta & 0xFF);
      my_snprintf(typestr, typestr_length, "BIT(%d)", nbits);
      if (!ptr) return fprintf(stdout, "NULL");
      length = (nbits + 7) / 8;
      my_b_write_bit(ptr, nbits);
      return length;
    }

    case MYSQL_TYPE_TIMESTAMP: {
      my_snprintf(typestr, typestr_length, "TIMESTAMP");
      if (!ptr) return fprintf(stdout, "NULL");
      uint32 i32 = uint4korr(ptr);
      fprintf(stdout, "%d", i32);
      return 4;
    }

    case MYSQL_TYPE_TIMESTAMP2: {
      my_snprintf(typestr, typestr_length, "TIMESTAMP(%d)", meta);
      if (!ptr) return fprintf(stdout, "NULL");
      char buf[MAX_DATE_STRING_REP_LENGTH];
      struct timeval tm;
      my_timestamp_from_binary(&tm, ptr, meta);
      int buflen = my_timeval_to_str(&tm, buf, meta);
      fprintf(stdout, buf, buflen);
      return my_timestamp_binary_length(meta);
    }

    case MYSQL_TYPE_DATETIME: {
      my_snprintf(typestr, typestr_length, "DATETIME");
      if (!ptr) return fprintf(stdout, "NULL");
      size_t d, t;
      uint64 i64 = uint8korr(ptr); /* YYYYMMDDhhmmss */
      d = static_cast<size_t>(i64 / 1000000);
      t = i64 % 1000000;
      fprintf(stdout, "%04d-%02d-%02d %02d:%02d:%02d",
              static_cast<int>(d / 10000), static_cast<int>(d % 10000) / 100,
              static_cast<int>(d % 100), static_cast<int>(t / 10000),
              static_cast<int>(t % 10000) / 100, static_cast<int>(t % 100));
      return 8;
    }

    case MYSQL_TYPE_DATETIME2: {
      my_snprintf(typestr, typestr_length, "DATETIME(%d)", meta);
      if (!ptr) return fprintf(stdout, "NULL");
      char buf[MAX_DATE_STRING_REP_LENGTH];
      MYSQL_TIME ltime;
      longlong packed = my_datetime_packed_from_binary(ptr, meta);
      TIME_from_longlong_datetime_packed(&ltime, packed);
      int buflen = my_datetime_to_str(&ltime, buf, meta);
      my_b_write_quoted((uchar *)buf, buflen);
      return my_datetime_binary_length(meta);
    }

    case MYSQL_TYPE_TIME: {
      my_snprintf(typestr, typestr_length, "TIME");
      if (!ptr) return fprintf(stdout, "NULL");
      uint32 i32 = uint3korr(ptr);
      fprintf(stdout, "'%02d:%02d:%02d'", i32 / 10000, (i32 % 10000) / 100,
              i32 % 100);
      return 3;
    }

    case MYSQL_TYPE_TIME2: {
      my_snprintf(typestr, typestr_length, "TIME(%d)", meta);
      if (!ptr) return fprintf(stdout, "NULL");
      char buf[MAX_DATE_STRING_REP_LENGTH];
      MYSQL_TIME ltime;
      longlong packed = my_time_packed_from_binary(ptr, meta);
      TIME_from_longlong_time_packed(&ltime, packed);
      int buflen = my_time_to_str(&ltime, buf, meta);
      my_b_write_quoted((uchar *)buf, buflen);
      return my_time_binary_length(meta);
    }

    case MYSQL_TYPE_NEWDATE: {
      my_snprintf(typestr, typestr_length, "DATE");
      if (!ptr) return fprintf(stdout, "NULL");
      uint32 tmp = uint3korr(ptr);
      int part;
      char buf[11];
      char *pos = &buf[10];  // start from '\0' to the beginning

      /* Copied from field.cc */
      *pos-- = 0;  // End NULL
      part = (int)(tmp & 31);
      *pos-- = (char)('0' + part % 10);
      *pos-- = (char)('0' + part / 10);
      *pos-- = ':';
      part = (int)(tmp >> 5 & 15);
      *pos-- = (char)('0' + part % 10);
      *pos-- = (char)('0' + part / 10);
      *pos-- = ':';
      part = (int)(tmp >> 9);
      *pos-- = (char)('0' + part % 10);
      part /= 10;
      *pos-- = (char)('0' + part % 10);
      part /= 10;
      *pos-- = (char)('0' + part % 10);
      part /= 10;
      *pos = (char)('0' + part);
      fprintf(stdout, "'%s'", buf);
      return 3;
    }

    case MYSQL_TYPE_YEAR: {
      my_snprintf(typestr, typestr_length, "YEAR");
      if (!ptr) return fprintf(stdout, "NULL");
      uint32 i32 = *ptr;
      fprintf(stdout, "%04d", i32 + 1900);
      return 1;
    }

    case MYSQL_TYPE_ENUM:
      switch (meta & 0xFF) {
        case 1:
          my_snprintf(typestr, typestr_length, "ENUM(1 byte)");
          if (!ptr) return fprintf(stdout, "NULL");
          fprintf(stdout, "%d", (int)*ptr);
          return 1;
        case 2: {
          my_snprintf(typestr, typestr_length, "ENUM(2 bytes)");
          if (!ptr) return fprintf(stdout, "NULL");
          int32 i32 = uint2korr(ptr);
          fprintf(stdout, "%d", i32);
          return 2;
        }
        default:
          fprintf(stdout, "!! Unknown ENUM packlen=%d", meta & 0xFF);
          return 0;
      }
      break;

    case MYSQL_TYPE_SET:
      my_snprintf(typestr, typestr_length, "SET(%d bytes)", meta & 0xFF);
      if (!ptr) return fprintf(stdout, "NULL");
      my_b_write_bit(ptr, (meta & 0xFF) * 8);
      return meta & 0xFF;

    case MYSQL_TYPE_BLOB:
      switch (meta) {
        case 1:
          my_snprintf(typestr, typestr_length, "TINYBLOB/TINYTEXT");
          if (!ptr) return fprintf(stdout, "NULL");
          length = *ptr;
          my_b_write_quoted(ptr + 1, length);
          return length + 1;
        case 2:
          my_snprintf(typestr, typestr_length, "BLOB/TEXT");
          if (!ptr) return fprintf(stdout, "NULL");
          length = uint2korr(ptr);
          my_b_write_quoted(ptr + 2, length);
          return length + 2;
        case 3:
          my_snprintf(typestr, typestr_length, "MEDIUMBLOB/MEDIUMTEXT");
          if (!ptr) return fprintf(stdout, "NULL");
          length = uint3korr(ptr);
          my_b_write_quoted(ptr + 3, length);
          return length + 3;
        case 4:
          my_snprintf(typestr, typestr_length, "LONGBLOB/LONGTEXT");
          if (!ptr) return fprintf(stdout, "NULL");
          length = uint4korr(ptr);
          my_b_write_quoted(ptr + 4, length);
          return length + 4;
        default:
          fprintf(stdout, "!! Unknown BLOB packlen=%d", length);
          return 0;
      }

    case MYSQL_TYPE_VARCHAR:
    case MYSQL_TYPE_VAR_STRING:
      length = meta;
      my_snprintf(typestr, typestr_length, "VARSTRING(%d)", length);
      if (!ptr) return fprintf(stdout, "NULL");
      return my_b_write_quoted_with_length(ptr, length);

    case MYSQL_TYPE_STRING:
      my_snprintf(typestr, typestr_length, "STRING(%d)", length);
      if (!ptr) return fprintf(stdout, "NULL");
      return my_b_write_quoted_with_length(ptr, length);

    case MYSQL_TYPE_JSON:
      my_snprintf(typestr, typestr_length, "JSON");
      if (!ptr) return fprintf(stdout, "NULL");
      length = uint2korr(ptr);
      my_b_write_quoted(ptr + meta, length);
      return length + meta;

    default: {
      char tmp[5];
      my_snprintf(tmp, sizeof(tmp), "%04x", meta);
      fprintf(stdout, "!! Don't know how to handle column type=%d meta=%d (%s)",
              type, meta, tmp);
    } break;
  }
  *typestr = 0;
  return 0;
}

Rows_log_event_ex::Rows_log_event_ex(
    const char *buf, uint event_len,
    const Format_description_event *description_event)
    : binary_log::Rows_event(buf, event_len, description_event),
      Rows_log_event(buf, event_len, description_event) {
  DBUG_ASSERT(header()->type_code == m_type);
}

Rows_log_event_ex::~Rows_log_event_ex() {}

/**
  Print a row event into IO cache in human readable form (in SQL format)

  @param[in] file              IO cache
  @param[in] print_event_into  Print parameters
*/
void Rows_log_event_ex::print_verbose(PRINT_EVENT_INFO *print_event_info) {
  // Quoted length of the identifier can be twice the original length
  char quoted_db[1 + NAME_LEN * 2 + 2];
  char quoted_table[1 + NAME_LEN * 2 + 2];
  size_t quoted_db_len, quoted_table_len;
  Table_map_log_event *map;
  table_def *td;
  const char *sql_command, *sql_clause1, *sql_clause2;
  Log_event_type general_type_code = get_general_type_code();

  if (m_extra_row_data) {
    uint8 extra_data_len = m_extra_row_data[EXTRA_ROW_INFO_LEN_OFFSET];
    uint8 extra_payload_len = extra_data_len - EXTRA_ROW_INFO_HDR_BYTES;
    assert(extra_data_len >= EXTRA_ROW_INFO_HDR_BYTES);

    RDP_LOG_DBG << "### Extra row data format: " << m_extra_row_data[EXTRA_ROW_INFO_FORMAT_OFFSET] << 
      ", len: "<< extra_payload_len << " :";
    if (extra_payload_len) {
      /*
         Buffer for hex view of string, including '0x' prefix,
         2 hex chars / byte and trailing 0
      */
      const int buff_len = 2 + (256 * 2) + 1;
      char buff[buff_len];
      str_to_hex(buff,
                 (const char *)&m_extra_row_data[EXTRA_ROW_INFO_HDR_BYTES],
                 extra_payload_len);
      RDP_LOG_DBG << buff;
    }
    RDP_LOG_DBG << "\n";
  }

  switch (general_type_code) {
    case binary_log::WRITE_ROWS_EVENT:
      sql_command = "INSERT INTO";
      sql_clause1 = "### SET\n";
      sql_clause2 = NULL;
      break;
    case binary_log::DELETE_ROWS_EVENT:
      sql_command = "DELETE FROM";
      sql_clause1 = "### WHERE\n";
      sql_clause2 = NULL;
      break;
    case binary_log::UPDATE_ROWS_EVENT:
      sql_command = "UPDATE";
      sql_clause1 = "### WHERE\n";
      sql_clause2 = "### SET\n";
      break;
    default:
      sql_command = sql_clause1 = sql_clause2 = NULL;
      DBUG_ASSERT(0); /* Not possible */
  }

  if (!(map = print_event_info->m_table_map.get_table(m_table_id)) ||
      !(td = map->create_table_def())) {
    char llbuff[22];
    RDP_LOG_DBG << "### Row event for unknown table #" << llstr(m_table_id, llbuff);
    return;
  }

  /* If the write rows event contained no values for the AI */
  if (((general_type_code == binary_log::WRITE_ROWS_EVENT) &&
       (m_rows_buf == m_rows_end))) {
    RDP_LOG_DBG << "### INSERT INTO `"<<map->get_db_name()<< "`.`" << map->get_table_name() << "` VALUES ()\n";
    goto end;
  }

  for (const uchar *value = m_rows_buf; value < m_rows_end;) {
    size_t length;
    quoted_db_len =
        my_strmov_quoted_identifier((char *)quoted_db, map->get_db_name());
    quoted_table_len = my_strmov_quoted_identifier((char *)quoted_table,
                                                   map->get_table_name());
    quoted_db[quoted_db_len] = '\0';
    quoted_table[quoted_table_len] = '\0';
    RDP_LOG_DBG << "### " << sql_command << " " << quoted_db << "." << quoted_table << "\n";
    /* Print the first image */
    if (!(length = print_verbose_one_row(td, print_event_info, &m_cols, value,
                                         (const uchar *)sql_clause1)))
      goto end;
    value += length;

    /* Print the second image (for UPDATE only) */
    if (sql_clause2) {
      if (!(length = print_verbose_one_row(td, print_event_info, &m_cols_ai,
                                           value, (const uchar *)sql_clause2)))
        goto end;
      value += length;
    }
  }

end:
  delete td;
}

/**
  Print a packed row into IO cache

  @param[in] file              IO cache
  @param[in] td                Table definition
  @param[in] print_event_into  Print parameters
  @param[in] cols_bitmap       Column bitmaps.
  @param[in] value             Pointer to packed row
  @param[in] prefix            Row's SQL clause ("SET", "WHERE", etc)

  @retval   - number of bytes scanned.
*/
size_t Rows_log_event_ex::print_verbose_one_row(
    table_def *td, PRINT_EVENT_INFO *print_event_info, MY_BITMAP *cols_bitmap,
    const uchar *value, const uchar *prefix) {
  const uchar *value0 = value;
  const uchar *null_bits = value;
  uint null_bit_index = 0;
  char typestr[64] = "";

  /*
    Skip metadata bytes which gives the information about nullabity of master
    columns. Master writes one bit for each affected column.
   */
  value += (bitmap_bits_set(cols_bitmap) + 7) / 8;

  RDP_LOG_DBG << prefix;

  for (size_t i = 0; i < td->size(); i++) {
    int is_null =
        (null_bits[null_bit_index / 8] >> (null_bit_index % 8)) & 0x01;

    if (bitmap_is_set(cols_bitmap, i) == 0) continue;

    RDP_LOG_DBG << "###   @" << static_cast<int>(i + 1) << "=";
    if (!is_null) {
      size_t fsize = td->calc_field_size((uint)i, (uchar *)value);
      if (value + fsize > m_rows_end) {
        RDP_LOG_DBG <<
            "***Corrupted replication event was detected."
            " Not printing the value***\n";
        value += fsize;
        return 0;
      }
    }
    size_t size =
        log_event_print_value(is_null ? NULL : value, td->type(i),
                              td->field_metadata(i), typestr, sizeof(typestr));
    if (!size) return 0;

    if (!is_null) value += size;

    if (print_event_info->verbose > 1) {
      RDP_LOG_DBG << " /* ";

      RDP_LOG_DBG << typestr;

      RDP_LOG_DBG << "meta=" << td->field_metadata(i) << 
        " nullable=" << td->maybe_null(i) << 
        " is_null=" << is_null << " ";
      RDP_LOG_DBG << "*/";
    }

    RDP_LOG_DBG << "\n";

    null_bit_index++;
  }
  return value - value0;
}

}  // namespace syncer
