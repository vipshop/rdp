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

#ifndef _ROWS_LOG_EVENT_EX_H_
#define _ROWS_LOG_EVENT_EX_H_

#include "syncer_consts.h"
#include "syncer_def.h"
#include "syncer_incl.h"
#include "syncer_main.h"

#include "../sql/my_decimal.h"
#include "../sql/sql_const.h"
#include "binary_log.h"
#include "hash.h"             // HASH
#include "my_atomic.h"        // my_atomic_add32
#include "my_bitmap.h"        // MY_BITMAP
#include "prealloced_array.h" // Prealloced_array
#include "sql_string.h"

#ifdef __cplusplus
extern "C" {
#endif

#include "decimal.h"

#ifdef __cplusplus
}
#endif

namespace syncer {

class Rows_log_event_ex : public Rows_log_event {

public:
  /*
    Constructor used by slave to read the event from the binary log.
   */
  explicit Rows_log_event_ex(const char *buf, uint event_len,
                             const Format_description_event *description_event);
  ~Rows_log_event_ex();

public:
  /**
    Print a row event into IO cache in human readable form (in SQL format)

    @param[in] file              IO cache
    @param[in] print_event_into  Print parameters
  */
  void print_verbose(PRINT_EVENT_INFO *print_event_info);

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
  size_t print_verbose_one_row(table_def *td,
                               PRINT_EVENT_INFO *print_event_info,
                               MY_BITMAP *cols_bitmap, const uchar *value,
                               const uchar *prefix);
};

} // namespace syncer

#endif // _ROWS_LOG_EVENT_EX_H_
