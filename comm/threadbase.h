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

#ifndef RDP_COMM_THREADBASE_H
#define RDP_COMM_THREADBASE_H

#include "comm_incl.h"

namespace rdp_comm {

class ThreadBase {
 public:
  explicit ThreadBase();
  virtual ~ThreadBase();

  bool Start(void);
  inline pthread_t GetThreadId(void) { return tid_; }
  inline pthread_attr_t GetThreadAttr(void) { return attr_; }

 protected:
  static void* RunProxy(void* ctx);
  virtual void Run(void) = 0;

 private:
  pthread_t tid_;
  pthread_attr_t attr_;
};

}  // namespace rdp_comm
#endif  // RDP_COMM_THREADBASE_H
