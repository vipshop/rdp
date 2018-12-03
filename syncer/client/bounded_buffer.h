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

#ifndef _BOUNDED_BUFFER_H_
#define _BOUNDED_BUFFER_H_

#include <rdp-comm/util.h>

#include "syncer_incl.h"
#include "syncer_def.h"
#include "statistics_writer.h"

namespace syncer {

class BoundedBuffer {

public:
  explicit BoundedBuffer(uint32_t capacity);
  ~BoundedBuffer(void);

public:
  void Push(void *p);
  void *Pop(void);
  uint32_t Capacity() { return capacity_; }
  uint32_t Size() { return item_count_; }

private:
  inline void Put(void *p);
  inline void *Get(void);

private:
  pthread_cond_t empty_cond_;
  pthread_cond_t put_cond_;
  pthread_mutex_t mutex_;

private:
  uint32_t capacity_;

  void **buffer_;
  uint32_t put_cur_;
  uint32_t get_cur_;
  uint32_t item_count_;
};

extern BoundedBuffer *g_event_bounded_buffer;
extern BoundedBuffer *g_trx_bounded_buffer;

} // namespace syncer

#endif // _BOUNDED_BUFFER_H_
