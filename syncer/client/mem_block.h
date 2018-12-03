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

#ifndef _MEM_BLOCK_H_
#define _MEM_BLOCK_H_

#include "syncer_incl.h"

namespace syncer {

class MemBlock {
public:
  explicit MemBlock();
  ~MemBlock();

  int Assign(char *buf, uint32_t size);
  char At(uint32_t index);
  bool GetBit(uint32_t pos);
  uint32_t GetBitsetCount();
  uint32_t GetBitsetCount(uint32_t size);

public:
  uint32_t size_;
  char    *block_;
};

} // namespace syncer

#endif // _MEM_BLOCK_H_
