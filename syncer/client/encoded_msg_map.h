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

#ifndef _ENCODED_MSG_MAP_H_
#define _ENCODED_MSG_MAP_H_

#include <rdp-comm/signal_handler.h>
#include <rdp-comm/logger.h>

#include "syncer_conf.h"
#include "syncer_incl.h"
#include "statistics_writer.h"
#include "split_package.h"

namespace syncer {

class EncodedMsgMap {

  typedef std::map<uint64_t, void *> MsgMap;

  enum {
    DEF_MAX_MSG_SIZE = 50000
  };

 public:
  explicit EncodedMsgMap(void);
  ~EncodedMsgMap(void);

 public:
  // \brief AddMsg Push an encoded msg to map
  //
  // \param seq_no
  // \param msg
  void AddMsg(uint64_t seq_no, void *msg);
  // \brief PopMsg Pop an encoded msg from map
  //
  // \param seq_no
  //
  // \return
  void PopMsg(uint64_t seq_no, vector<SeqTrans *> &seq_trans,
              const int max_trans);
  // \brief GetMsgSize Get msg size
  //
  // \return
  size_t GetMsgSize(void);

  // \brief SetCapacity set capacity_
  //
  // \param uint32_t
  void SetCapacity(const uint32_t capacity);

 private:
  MsgMap msg_map_;

 private:
  pthread_cond_t add_cond_;
  pthread_cond_t empty_cond_;
  pthread_mutex_t mutex_;

  uint64_t wait_seq_no_;

  uint32_t capacity_;
};

extern EncodedMsgMap g_encode_msg_map;

}  // namespace syncer

#endif  // _ENCODED_MSG_MAP_H_
