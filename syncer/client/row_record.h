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

#ifndef _ROW_H_
#define _ROW_H_

#include "syncer_incl.h"
#include "mem_block.h"
using std::vector;
using std::string;

namespace syncer {

enum EnumAction { INSERT, UPDATE, DEL };

// 存储解析后的行数据
struct Row {
public:
  explicit Row(void){};
  Row(const Row &row);
  ~Row(void){};

public:
  int Init(EnumAction eAction, std::string sDatabaseName,
           std::string sTableName);
  int PushBack(uint32_t column_idx, char *pBuf, bool bOld);
  int PushBack(uint32_t column_idx, std::string sBuf, bool bOld);
  int PushBack(uint32_t column_idx, char *pBuf, uint32_t uLen, bool bOld);
  std::string &GetColumnByIndex(uint32_t uIndex, bool bOld);

public:
  int Print();

public:
  EnumAction action_;
  MemBlock null_bitmap_;
  MemBlock old_null_bitmap_;
  std::map<uint32_t, std::string> column_;
  std::map<uint32_t, std::string> old_column_;
  std::string databasename_;
  std::string tablename_;
  std::string temp_;
};

} // namespace syncer

#endif // _ROW_H_
