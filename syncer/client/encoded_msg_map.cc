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

#include "encoded_msg_map.h"

namespace syncer {

EncodedMsgMap g_encode_msg_map;

EncodedMsgMap::EncodedMsgMap(void)
    : wait_seq_no_(0), capacity_(DEF_MAX_MSG_SIZE) {
  PthreadCall("pthread_mutex_init", pthread_mutex_init(&mutex_, NULL));
  PthreadCall("pthread_cond_init", pthread_cond_init(&add_cond_, NULL));
  PthreadCall("pthread_cond_init", pthread_cond_init(&empty_cond_, NULL));
}

EncodedMsgMap::~EncodedMsgMap() {
  PthreadCall("pthread_mutex_destroy", pthread_mutex_destroy(&mutex_));
  PthreadCall("pthread_cond_destroy", pthread_cond_destroy(&add_cond_));
  PthreadCall("pthread_cond_destroy", pthread_cond_destroy(&empty_cond_));
}

// Push an encoded msg to map
void EncodedMsgMap::AddMsg(uint64_t seq_no, void *msg) {
  PthreadCall("pthread_mutex_lock", pthread_mutex_lock(&mutex_));

  if (0 == msg_map_.size() % 100 && 0 != msg_map_.size()) {
    RDP_LOG_DBG << "Encoded Msg Map Size: " << msg_map_.size();
  }

  // wait PopMsg if msg_map_ size is more then capacity_
  // but if seq no is wait_seq_no_, add
  while (seq_no != wait_seq_no_ && msg_map_.size() >= capacity_) {
    RDP_LOG_WARN << "Encoded Msg Map Size: " << msg_map_.size()
                 << ", More Then capacity_: " << capacity_ << ", Block";
    PthreadCall("pthread_cond_wait", pthread_cond_wait(&empty_cond_, &mutex_));
  }

  msg_map_[seq_no] = msg;

  RDP_LOG_DBG << "EncodedMsgMap Add Seq No: " << seq_no;

  PthreadCall("pthread_cond_signal", pthread_cond_signal(&add_cond_));
  PthreadCall("pthread_mutex_unlock", pthread_mutex_unlock(&mutex_));
}

// Pop an encoded msg from map
void EncodedMsgMap::PopMsg(uint64_t seq_no, vector<SeqTrans *> &seq_trans, const int max_trans) {
  PthreadCall("pthread_mutex_lock", pthread_mutex_lock(&mutex_));
  if (msg_map_.find(seq_no) == msg_map_.end()) {
    // not found, set seq no waiting for
    wait_seq_no_ = seq_no;
    PthreadCall("pthread_cond_signal", pthread_cond_signal(&empty_cond_));

    struct timeval now;
    struct timespec outtime;

    gettimeofday(&now, NULL);

    // wait 1 second
    outtime.tv_sec = now.tv_sec + 1;
    outtime.tv_nsec = now.tv_usec * 1000;

    // wait
    int rs = pthread_cond_timedwait(&add_cond_, &mutex_, &outtime);
    switch (rs) {
      // add_conf signal
      case 0:
        // msg add, but maybe not seq_no msg
        if (msg_map_.find(seq_no) == msg_map_.end()) {
          PthreadCall("pthread_mutex_unlock", pthread_mutex_unlock(&mutex_));
          return ;
        }
        break;

      // outtime timeout
      case ETIMEDOUT:
        PthreadCall("pthread_mutex_unlock", pthread_mutex_unlock(&mutex_));
        return ;

      case EINVAL:
      // The mutex was not owned by the current thread at the time of the call.
      // this should not happend
      case EPERM:
      // should not reach default
      default:
        PthreadCall("pthread_mutex_unlock", pthread_mutex_unlock(&mutex_));
        RDP_LOG_ERROR << "Pthread_cond_timedwait Return: " << rs
                   << ", Error: " << rdp_comm::GetLastErrorMsg(errno);
        return ;
    }
  }

  RDP_LOG_DBG << "EncodedMsgMap Pop Seq No: " << seq_no;

  MsgMap::iterator begin = msg_map_.find(seq_no);
  MsgMap::iterator curr = begin;
  for (; curr != msg_map_.end() && wait_seq_no_ == curr->first && seq_trans.size() < (unsigned int)max_trans; curr++, wait_seq_no_++) {
    seq_trans.push_back((syncer::SeqTrans*)curr->second);
  }
  msg_map_.erase(begin, curr);

  PthreadCall("pthread_cond_signal", pthread_cond_signal(&empty_cond_));
  PthreadCall("pthread_mutex_unlock", pthread_mutex_unlock(&mutex_));

  return ;
}

size_t EncodedMsgMap::GetMsgSize() {
  size_t rs;
  PthreadCall("pthread_mutex_lock", pthread_mutex_lock(&mutex_));
  rs = msg_map_.size();
  PthreadCall("pthread_mutex_unlock", pthread_mutex_unlock(&mutex_));
  return rs;
}

void EncodedMsgMap::SetCapacity(const uint32_t capacity) {
  PthreadCall("pthread_mutex_lock", pthread_mutex_lock(&mutex_));
  capacity_ = capacity;
  PthreadCall("pthread_mutex_unlock", pthread_mutex_unlock(&mutex_));
}

}  // namespace syncer
