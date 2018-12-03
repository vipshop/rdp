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

#include "mem_block.h"

namespace syncer {

MemBlock::MemBlock() {
  size_ = 0;
  block_ = NULL;
}

MemBlock::~MemBlock() {
  if (block_ != NULL) {
    delete []block_;
    block_ = NULL;
    size_ = 0;
  }
}

int MemBlock::Assign(char *buf, uint32_t size) {
  if (block_ != NULL) {
    delete block_;
    block_ = NULL;
    size_ = 0;
  }

  block_ = new char[size];
  if (block_ == NULL)
    return -1;

  memcpy(block_, buf, size);
  size_ = size;

  return 0;
}

char MemBlock::At(uint32_t index) {
  assert(index < size_);
  return *(block_ + index);
}

// 判断某个bit位是否为 1
bool MemBlock::GetBit(uint32_t pos) {
  uint32_t mchar = pos / 8;   //字节序号
  uint32_t nbit = pos & 0x07; //字节中的bit序号
  assert(mchar < size_);
  return ((block_[mchar] >> nbit) & 0x01) == 0x01 ? true : false;
}

uint32_t MemBlock::GetBitsetCount() {
  uint32_t count = 0;
  for (uint32_t i = 0; i < size_; i++) {
    uint8_t p = *(block_ + i);
    while (p != 0) {
      if ((p & 0x01) != 0)
        count++;
      p = p >> 1;
    }
  }

  return count;
}

// 获取 0~bitset_size 范围内 bit位为 1 的位数
uint32_t MemBlock::GetBitsetCount(uint32_t bitset_size) {
  uint32_t count = 0;
  assert(bitset_size <= size_ * 8);
  for (uint32_t i = 0; i < bitset_size; i++) {
    if (GetBit(i))
      count++;
  }

  return count;
}

} // namespace syncer
