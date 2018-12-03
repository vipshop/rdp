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

#ifndef __RATE_LIMITER_H__
#define __RATE_LIMITER_H__


#include <inttypes.h>

namespace syncer {

  class RateLimiter {
    public:
      /**
       *
       * @param rate the number of tokens add to the bucket per ms
       * @param max_tokens the max number of tokens the bucket can hold, 
       *        default is 10s * rate
       */
      RateLimiter(uint64_t rate, uint64_t max_tokens);
      RateLimiter(uint64_t rate);

      // Wait until the bucket have n tokens at least
      void Wait(uint64_t n);

    private:
      uint64_t GetCurrentMs();

      // Max tokens this bucket can hold
      uint64_t max_tokens_;
      uint64_t tokens_;

      // How many tokens add to the bucket per ms
      uint64_t rate_;


      uint64_t last_time_;
  };


}


#endif
