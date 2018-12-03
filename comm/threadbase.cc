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

#include "threadbase.h"
#include "signal_handler.h"

namespace rdp_comm {

ThreadBase::ThreadBase() {
  pthread_attr_init(&attr_);
  pthread_attr_setdetachstate(&attr_, PTHREAD_CREATE_JOINABLE);
  pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);
  memset(&tid_, 0, sizeof(pthread_t));
}

ThreadBase::~ThreadBase() { pthread_attr_destroy(&attr_); }

bool ThreadBase::Start() {
  int ret = pthread_create(&tid_, &attr_, &ThreadBase::RunProxy, this);
  if (0 != ret) {
    LOG(ERROR) << "Create Thread Failt: " << GetLastErrorMsg(ret);
    return false;
  }

  return true;
}

void *ThreadBase::RunProxy(void *ctx) {
  if (NULL != ctx) {
    ThreadBase *t = static_cast<ThreadBase *>(ctx);
    t->Run();
  }

  return NULL;
}

}  // namespace rdp_comm
