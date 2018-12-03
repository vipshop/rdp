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

#include "syncer_def.h"
#include "bounded_buffer.h"

namespace syncer {

BoundedBuffer::BoundedBuffer(uint32_t capacity)
    : capacity_(capacity), put_cur_(0), get_cur_(0), item_count_(0) {
  PthreadCall("pthread_mutex_init", pthread_mutex_init(&mutex_, NULL));
  PthreadCall("pthread_cond_init", pthread_cond_init(&empty_cond_, NULL));
  PthreadCall("pthread_cond_init", pthread_cond_init(&put_cond_, NULL));
  buffer_ = (void **)malloc(sizeof(void *) * capacity_);
  assert(buffer_);
}

BoundedBuffer::~BoundedBuffer() {
  free(buffer_);
  PthreadCall("pthread_cond_destroy", pthread_cond_destroy(&put_cond_));
  PthreadCall("pthread_cond_destroy", pthread_cond_destroy(&empty_cond_));
  PthreadCall("pthread_mutex_destroy", pthread_mutex_destroy(&mutex_));
}

void BoundedBuffer::Put(void *p) {
  buffer_[put_cur_] = p;
  put_cur_ = (put_cur_ + 1) % capacity_;
  ++item_count_;
}

void *BoundedBuffer::Get(void) {
  void *p = buffer_[get_cur_];
  buffer_[get_cur_] = NULL;
  get_cur_ = (get_cur_ + 1) % capacity_;
  --item_count_;
  return p;
}

void BoundedBuffer::Push(void *p) {
  PthreadCall("pthread_mutex_lock", pthread_mutex_lock(&mutex_));
  while (item_count_ == capacity_) {
    PthreadCall("pthread_cond_wait", pthread_cond_wait(&empty_cond_, &mutex_));
  }
  this->Put(p);
  PthreadCall("pthread_cond_signal", pthread_cond_signal(&put_cond_));
  PthreadCall("pthread_mutex_unlock", pthread_mutex_unlock(&mutex_));
}

void *BoundedBuffer::Pop(void) {
  void *tmp = NULL;
  PthreadCall("pthread_mutex_lock", pthread_mutex_lock(&mutex_));
  while (item_count_ == 0) {
    PthreadCall("pthread_cond_wait", pthread_cond_wait(&put_cond_, &mutex_));
  }
  tmp = this->Get();
  PthreadCall("pthread_cond_signal", pthread_cond_signal(&empty_cond_));
  PthreadCall("pthread_mutex_unlock", pthread_mutex_unlock(&mutex_));

  return tmp;
}

BoundedBuffer *g_event_bounded_buffer;
BoundedBuffer *g_trx_bounded_buffer;

}  // namespace syncer
