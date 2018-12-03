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

#ifndef _CHECKSUM_H_
#define _CHECKSUM_H_

#include <boost/crc.hpp>
#include <rdp-comm/util.h>
#include "syncer_incl.h"

namespace syncer {

class CheckSum {
 private:
  CheckSum() {}
  ~CheckSum() {}

 public:
  static int64_t Get(void const *buffer, std::size_t byte_count);

 private:
  static boost::crc_32_type crc32_;
  static rdp_comm::spinlock_t lock_;
};

}  // namespace syncer
#endif  // _CHECKSUM_H_
