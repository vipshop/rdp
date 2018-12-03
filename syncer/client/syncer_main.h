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

#ifndef _SYNCER_MAIN_H_
#define _SYNCER_MAIN_H_

#define MYSQL_CLIENT
#undef MYSQL_SERVER
#ifndef HAVE_REPLICATION
#define HAVE_REPLICATION
#endif
#include "client_priv.h"
#include "my_default.h"
#include <my_time.h>
//#include <sslopt-vars.h>
/* That one is necessary for defines of OPTION_NO_FOREIGN_KEY_CHECKS etc */
#include "../sql/query_options.h"
#include <signal.h>
#include <my_dir.h>
//#include "mysqld.h"            // UUID_LENGTH
//#include "protocol_classic.h"  // net_store_data
#include <mysqld_error.h>

#include "prealloced_array.h"

#include "../sql/rpl_gtid.h"
#include "../sql/log_event.h"
#include "../sql/log_event_old.h"
#include "sql_common.h"
#include "my_dir.h"
#include "my_sys.h" // crc32
#include <welcome_copyright_notice.h> // ORACLE_WELCOME_COPYRIGHT_NOTICE
#include "sql_string.h"
#include "../sql/my_decimal.h"
#include "../sql/rpl_constants.h"

#include <algorithm>
#include <utility>
#include <map>

using std::min;
using std::max;

#endif // _SYNCER_MAIN_H_
