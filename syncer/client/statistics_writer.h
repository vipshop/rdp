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

#ifndef _STATISTICS_WRITER_H_
#define _STATISTICS_WRITER_H_

#define __STDC_FORMAT_MACROS
#include <inttypes.h>

#include <rdp-comm/util.h>
#include "syncer_incl.h"

#ifdef STAT_TIME

#define STAT_TIME_POINT(type, seq_no, stat) StatTimePoint(type, seq_no, stat)
#define STAT_TIME_POINT_END(stat) StatTimePointEnd(stat)

#else

#define STAT_TIME_POINT(type, seq_no, stat) void(0);
#define STAT_TIME_POINT_END(stat) void(0);

#endif

using std::map;
using std::string;
using std::queue;

namespace syncer {

typedef struct StatPoint {
  uint64_t seq_no;
  uint64_t event_num;
  struct timeval t1;
  struct timeval t2;
  struct timeval t3;
  struct timeval t4;
  struct timeval t5;
  struct timeval t6;
  struct timeval t7;
  struct timeval t8;
  struct timeval t9;
  struct timeval t10;
  struct timeval t11;
  struct timeval t12;
  struct timeval t13;
  StatPoint() {
    memset(&t1, 0, sizeof(t1));
    memset(&t2, 0, sizeof(t2));
    memset(&t3, 0, sizeof(t3));
    memset(&t4, 0, sizeof(t4));
    memset(&t5, 0, sizeof(t5));
    memset(&t6, 0, sizeof(t6));
    memset(&t7, 0, sizeof(t7));
    memset(&t8, 0, sizeof(t8));
    memset(&t9, 0, sizeof(t9));
    memset(&t10, 0, sizeof(t10));
    memset(&t11, 0, sizeof(t11));
    memset(&t12, 0, sizeof(t12));
    memset(&t13, 0, sizeof(t13));
  }
} StatPoint;

/*
t1 开始接收数据
t2 接收完一个trx数据
t3 push到bound buffer完成
t4 pop bound buffer
t5 AssignTask加入worker 线程
t6 worker GetTask
t7 解析为transaction完成
t8 transaction序列化完成
t9 transaction序列化后 checksum crc32完成
t10 分包完成
t11 加入encode map完成
t12 ecnode map获取到
t13 transaction成功写入kafka
*/
enum StatIndicator {
  STAT_T1 = 0,
  STAT_T2,
  STAT_T3,
  STAT_T4,
  STAT_T5,
  STAT_T6,
  STAT_T7,
  STAT_T8,
  STAT_T9,
  STAT_T10,
  STAT_T11,
  STAT_T12,
  STAT_T13,
};

void StatTimePoint(StatIndicator type, uint64_t seq_no, StatPoint *stat);

void StatTimePointEnd(StatPoint *stat);

FILE* StatGetFileHandle(const string file_name);

bool StatWrite(const string data, FILE *fd);

void StatClose(const string file_name);

void StatCloseAll(void);

uint32_t StatGetIntervalMs(void);

}  // namespace syncer

#endif  // _STATISTICS_WRITER_H_

