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

#ifndef _SPLIT_PACKAGE_H_
#define _SPLIT_PACKAGE_H_

#define __STDC_FORMAT_MACROS
#include <inttypes.h>

#include "rdp.pb.h"
#include "syncer_incl.h"
#include "checksum.h"
#include "statistics_writer.h"
#include "syncer_def.h"

using std::string;
using std::vector;

typedef rdp::messages::Transaction Transaction;
typedef rdp::messages::KafkaPkg KafkaPkg;
typedef rdp::messages::VMSMessage VMSMessage;

namespace syncer {

enum PkgSplitFlag {
  PKG_NOT_SPLIT = 0,
  PKG_SPLIT = 1,
  PKG_SPLIT_END = 2
};

struct SeqKafkaPkg {
  // split kafka pkg seq_no
  uint64_t split_seq_no;
  // split kafka pkg flag
  PkgSplitFlag split_flag;
  // KafkaPkg Serialize buffer
  char *buffer;
  // buffer serizlize data length
  size_t buffer_size;
  // transaction info
  struct SeqTrans *trans;
  // write kafka pkg avg consumption time
  struct timeval sent;
  // recv write kafka response
  struct timeval recv_rsp;
  // kafka offset
  uint64_t offset;

  explicit SeqKafkaPkg(const uint64_t split_seq_no_p, PkgSplitFlag split_flag_p,
                       char *data, const size_t data_size, struct SeqTrans *trans_p);

  ~SeqKafkaPkg();

  // Get Key for kafka producer waiting for respnse map
  string Key(void);
};

struct SeqTrans {
  // transactions seq_no
  uint64_t trans_seq_no;
  // transaction gtid
  string gtid;
  // epoch 
  uint64_t epoch;

  uint64_t event_count;

  // split Serialize packages
  vector<struct SeqKafkaPkg *> pkgs;

  // original transaction
  rdp::messages::Transaction transaction;

  SeqTrans(const uint64_t seq_no, const string &gtid_p, const uint64_t epoch_p)
      : trans_seq_no(seq_no), gtid(gtid_p), epoch(epoch_p) {
    pkgs.clear();
#ifdef STAT_TIME
    stat_point = NULL;
#endif
  }

#ifdef STAT_TIME
  StatPoint *stat_point;
#endif
  void ClearPkgs() {
    vector<struct SeqKafkaPkg *>::iterator pkg_it = this->pkgs.begin();
    for (; pkg_it != this->pkgs.end(); ++pkg_it) {
      assert(*pkg_it != NULL);
      delete (*pkg_it);
    }
    this->pkgs.clear();
  }
};

struct SeqTransCtx {
  size_t split_msg_bytes;
  bool enable_checksum;
  bool enable_compress;
  bool enable_trans_clean;
  bool enable_split_msg;
  size_t kafka_pkg_head_size;
  // 记录transaction序列化数据
  char *transaction_buf;
  size_t transaction_buf_size;
  // 记录压缩后数据
  char *compress_buf;
  size_t compress_buf_size;
  // 记录需要发送到kafka的最终数据
  char *dest_buf;
  size_t dest_buf_size;

  SeqTransCtx(const size_t p_split_msg_bytes, const bool p_enable_checksum,
              const bool p_enable_compress, const bool p_enable_split_msg,
              const bool p_enable_trans_clean, const size_t default_buf_size);
  ~SeqTransCtx();
  void AllocTranBuf(const size_t need_size);
  void AllocCompressBuf(const size_t need_size);
  void AllocDestBuf(const size_t need_size);

  bool IsLESplitMsgSize(size_t s);
};

typedef struct SeqTrans SeqTrans;
typedef struct SeqKafkaPkg SeqKafkaPkg;
typedef struct SeqTransCtx SeqTransCtx;

size_t KafkaPkgHeadSize();

int ToSerialize(Transaction *transaction, SeqTransCtx *seq_trans_ctx,
                char *&serialize_data, size_t &serialize_data_len); 

SeqTrans *TransactionToSeqTrans(Transaction *transaction, SeqTransCtx *seq_trans_ctx, uint64_t trans_seq_no);
void ClearTransEventAlarm(rdp::messages::Transaction *transaction);
void ClearRowsInDataEvents(rdp::messages::Transaction *transaction); 
}  // namespace syncer
#endif  // _SPLIT_PACKAGE_H_
