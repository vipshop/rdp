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

#ifndef _RDP_JANSSON_PACK_H_
#define _RDP_JANSSON_PACK_H_

#include <jansson.h>
#include <glog/logging.h>
#include "syncer_incl.h"

using std::string;

namespace syncer {
class CPJansson {
 private:
  enum DefBufferSize {
    BUFFER_SIZE = 1024
  };

 private:
  CPJansson() {}
  ~CPJansson() {}

 public:
  // \brief Encode: encode to jansson string
  //
  // \param topic
  // \param offset
  // \param flag
  // \param timestamp
  // \param gtid
  // \param epoch
  // \param seq_no
  // \param rs
  //
  // \return true if success
  static bool Encode(const string &topic, const string &offset,
                     const string &flag, const string &timestamp,
                     const string &gtid, const string &epoch,
                     const string &seq_no, string *rs);

  // \brief Decode: decode jansson string
  //
  // \param source
  // \param topic
  // \param offset
  // \param flag
  // \param timestamp
  // \param gtid
  // \param epoch
  // \param seq_no
  //
  // \return true if success
  static bool Decode(const string &source, string *topic, string *offset,
                     string *flag, string *timestamp, string *gtid,
                     string *epoch, string *seq_no);

 private:
  // \brief Init json struct init
  //
  // \return
  static bool Init(void);

  // \brief AllTitleLen: get all title strngs length
  //
  // \param vod
  //
  // \return
  static uint32_t AllTitleLen(void);

 private:
  static json_t *json_;
  static json_t *json_topic_;
  static json_t *json_offset_;
  static json_t *json_flag_;
  static json_t *json_timestamp_;
  static json_t *json_gtid_;
  static json_t *json_epoch_;
  static json_t *json_seq_no_;

  static string title_topic_;
  static string title_offset_;
  static string title_flag_;
  static string title_timestamp_;
  static string title_gtid_;
  static string title_epoch_;
  static string title_seq_no_;

  static char *encode_buffer_;
  static uint32_t encode_buffer_size_;

  static bool init_flag_;
};

}  // namespace syncer
#endif  // _RDP_JANSSON_PACK_H_
