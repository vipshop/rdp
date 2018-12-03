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

#ifndef RDP_SRC_BROKEN_POINTS_H
#define RDP_SRC_BROKEN_POINTS_H

#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <string>

#include "continuity_check.h"

using std::string;

enum ErrType {
  TransErr = 0,
  EventErr,
};

typedef struct ErrOffset {
  uint64_t begin_offset;
  uint64_t end_offset;
  uint64_t epoch;
  ::rdp::messages::Transaction *trans;
  ErrOffset(uint64_t begin, uint64_t end, uint64_t p_epoch, ::rdp::messages::Transaction *p_trans) {
    begin_offset = begin;
    end_offset = end;
    epoch = p_epoch;
    trans = p_trans;
  }
  ErrOffset() {
    begin_offset = 0;
    end_offset = 0;
    epoch = 0;
    trans = NULL;
  }
  ~ErrOffset() {
    if (trans) {
      delete trans;
      trans = NULL;
    }
  }
} ErrOffset;

enum ErrSetType {
  ERR_BROKEN_POINT = 0,
  ERR_NOT_CONFIRM,
  ERR_NO_ERROR
};

typedef struct ErrOffsetSet {
  ErrSetType set_type;
  ErrType type;
  ErrOffset before;
  ErrOffset after;
} ErrOffsetSet;

void ErrPrint(void);
void AddErr(ErrType type, PackingMsg *before_msg, PackingMsg *after_msg);
bool ErrToJson(const string file, int64_t start_offset, int64_t stop_offset);

#endif // RDP_SRC_BROKEN_POINTS_H
