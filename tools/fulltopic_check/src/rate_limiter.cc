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

#include <sys/time.h>
#include <unistd.h>
#include "rate_limiter.h"

namespace syncer {

  RateLimiter::RateLimiter(uint64_t rate)
    : max_tokens_(10*1000*rate), tokens_(0), rate_(rate) {
      last_time_ = GetCurrentMs();
  }

  RateLimiter::RateLimiter(uint64_t rate, uint64_t max_tokens_)
    : max_tokens_(max_tokens_), tokens_(0), rate_(rate) {
      last_time_ = GetCurrentMs();
  }

  void RateLimiter::Wait(uint64_t n) {
    if (rate_ == 0) {
      // No limit 
      return;
    }

    if (n <= tokens_) {
      tokens_ -= n;
      return;
    }

    uint64_t now =  GetCurrentMs();
    tokens_ += (now - last_time_) * rate_;
    last_time_ = now;
    if (tokens_ > max_tokens_) {
      // It may long long ago when add tokens into bucket, trim it 
      tokens_ = max_tokens_;
    }

    while (true) {
      if (tokens_ >= n) {
        break;
      }

      uint64_t sleep_ms = (n - tokens_) / rate_ + 1;
      usleep(sleep_ms * 1000);

      uint64_t now =  GetCurrentMs();
      tokens_ += (now - last_time_) * rate_;
      last_time_ = now;
    }

    tokens_ -= n;
    if (tokens_ > max_tokens_) {
      tokens_ = max_tokens_;
    }
    
  }

  uint64_t RateLimiter::GetCurrentMs() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec*1000 + tv.tv_usec/1000 ;
  }

}
