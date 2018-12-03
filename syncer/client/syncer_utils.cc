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

#include <rdp-comm/logger.h>
#include "syncer_utils.h"
#include "syncer_main.h"
#include "boost/algorithm/string.hpp"

namespace syncer {

// Return the error message
std::string GetLastErrorMsg(int err) {
  char err_msg[128] = {0x0};
  return std::string(strerror_r(err, err_msg, sizeof(err_msg)));
}

void PthreadCall(const char* label, int result) {
  if (result != 0) {
    RDP_LOG_ERROR << label << ": " << GetLastErrorMsg(result);
    abort();
  }
}

// Synchronously send data with length
bool SyncSend(int fd, const uint8_t *data, int32_t size) {
  int sent = 0;
  while (sent < size) {
    errno = 0;
    int s = ::send(fd, (const char *)(data + sent), size - sent, 0);
    if (s > 0) {
      sent += s;
    } else {
      if (s == -1) {
        if (errno == EINTR || errno == EAGAIN)
          continue;

        RDP_LOG_ERROR << "send failed: " << GetLastErrorMsg(errno).c_str();
      } else if (s == 0) {
        RDP_LOG_ERROR << "connection broken @ send: " << GetLastErrorMsg(errno).c_str();
      }

      return false;
    }
  }

  return true;
}

bool UnpackLenencInt(char *pBuf, uint32_t &uPos, uint64_t &uValue) {
  uint8_t u8Type = pBuf[0];
  if (u8Type < 0xfb) {
    uValue = u8Type;
    uPos += 1;
  } else if (u8Type == 0xfc) {
    uPos += 3;
    uValue = uint2korr(pBuf + 1);
  } else if (u8Type == 0xfd) {
    uPos += 4;
    uValue = uint3korr(pBuf + 1);
  } else if (u8Type == 0xfe) {
    uPos += 9;
    uValue = uint8korr(pBuf + 1);
  } else {
    return false;
  }

  return true;
}

bool IsSystemSchema(const std::string db_name) {
  std::string curr_db_name = db_name;
  boost::algorithm::to_lower(curr_db_name);

  return curr_db_name == "sys" ||
         curr_db_name == "information_schema" ||
         curr_db_name == "mysql" || 
         curr_db_name == "performance_schema";  
}

bool IsDataEvent(uint32_t event_type) {
  if (event_type == binary_log::WRITE_ROWS_EVENT  ||  
      event_type == binary_log::UPDATE_ROWS_EVENT ||
      event_type == binary_log::DELETE_ROWS_EVENT) {
    return true;
  }

  return false;
}

uint32_t g_max_trans_size = 786432;

bool IsEventTruncated(uint32_t event_type, uint32_t msg_len) {
  if (IsDataEvent(event_type) &&   
      TRUNC_ROWS_EVENT_SIZE == msg_len) {
      return true;
  }

  return false;
}

std::string WriteBits(char *buf, uint32_t buf_size) {
  std::string dst;
  uint32_t nbits8= buf_size * 8;
  dst = "b'";
  for (uint32_t bitnum = 0; bitnum < nbits8; bitnum++)
  {
    int is_set= (buf[(bitnum) / 8] >> (7 - bitnum % 8))  & 0x01;
    dst += (is_set ? "1" : "0");
  }
  dst += "'";

  return dst;
}

uint64_t BFixedLengthInt(unsigned char *b, int length) {
  uint64_t num = 0;

  for (int i =0; i<length; i++ ) {
    num |= uint64_t(b[i]) << (uint32_t(length-i-1) * 8);
  }
  return num;
}

// Decode big-endian bytes into uint64
uint64_t DecodeBit(unsigned char *b, int length) {
  uint64_t value;
  switch (length) {
    case 1:
    case 2:
    case 3:
    case 4:
    case 5:
    case 6:
    case 7:
    case 8:
      value = BFixedLengthInt(b, length);
      break;
    default:
      // Unsupportted byte count
    abort();
  }
  return  value;
}

// Tranform to lower case
void Str2Lower(std::string& str) {
  for (unsigned int i = 0; i < str.size(); i++) {
    str[i] = tolower(str[i]);
  }
}

// Trim space at both end
std::string Trim(std::string const& source, char const *delims) {
  std::string result(source);
  std::string::size_type index = result.find_last_not_of(delims);
  if (index != std::string::npos)
    result.erase(++index);

  index = result.find_first_not_of(delims);
  if (index != std::string::npos)
    result.erase(0, index);
  else
    result.erase();

  return result;
}


} // namespace syncer
