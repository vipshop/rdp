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

#include "split_package.h"
#include "memory_pool.h"
#include "checkpoint.h"
#include "rdp_syncer_metric.h"
#include <boost/lexical_cast.hpp>
#include <lz4.h>
#include <list>

using std::list;

#define ALLOC_BUF(buf, need_size, buf_size)                   \
  do {                                                        \
    if ((buf) == NULL) {                                      \
      if ((buf_size) < (need_size)) (buf_size) = (need_size); \
      (buf) = (char *)malloc((need_size));                    \
      assert((buf) != NULL);                                  \
      break;                                                  \
    }                                                         \
    if ((need_size) > (buf_size)) {                           \
      (buf) = (char *)realloc((buf), (need_size));            \
      assert(NULL != (buf));                                  \
      (buf_size) = (need_size);                               \
    }                                                         \
  } while (0);

#define FREE_BUF(buf)    \
  do {                   \
    if ((buf) != NULL) { \
      free(buf);         \
      (buf) = NULL;      \
    }                    \
  } while (0);

namespace syncer {

static int Compress(const char *src, const size_t src_size, SeqTransCtx *seq_trans_ctx) {
#if 0
  struct timeval start;
  struct timeval end;
  gettimeofday(&start, NULL);
#endif
  // 获取压缩后空间
  const int max_dst_size = LZ4_compressBound(src_size);
  // 确保空间足够
  seq_trans_ctx->AllocCompressBuf(max_dst_size);
  int retry = 0;
  int compressed_data_size = 0;
  // 压缩 失败后重试
  do {
    compressed_data_size = LZ4_compress_default(src, seq_trans_ctx->compress_buf, src_size, seq_trans_ctx->compress_buf_size);
#if 0
  gettimeofday(&end, NULL);
#endif
    // 压缩失败
    if (compressed_data_size < 0)
      RDP_LOG_ERROR << "A negative result from LZ4_compress_default "
                       "indicates a failure trying to compress the data.  See exit code (echo "
                    << compressed_data_size << ") for value returned.";
    // 空间不足 压缩失败
    // 一般情况不会进入里面，因为LZ4_compressBound已经计算压缩后空间
    if (compressed_data_size == 0) {
      RDP_LOG_WARN << "A result of 0 means compression worked, but was stopped because the "
                      "destination buffer couldn't hold all the information.";
      seq_trans_ctx->AllocCompressBuf(seq_trans_ctx->compress_buf_size * 2);
    }
  } while (compressed_data_size == 0 && retry++ < 3);

  if (compressed_data_size == 0) {
    RDP_LOG_ERROR << "compress stopped because thedestination buffer couldn't hold all the "
                     "information. compress buf size: " << seq_trans_ctx->compress_buf_size;
  }
#if 0
  RDP_LOG_DBG << "Compress src size:" << src_size << " dst size:" << compressed_data_size 
    << " percentage:" << src_size * 1.0 / compressed_data_size << " Time:" 
    << (end.tv_usec - start.tv_usec + (end.tv_sec - start.tv_sec) * 1000 * 1000) << " us";
#else
  RDP_LOG_DBG << "Compress src size:" << src_size << " dst size:" << compressed_data_size
              << " percentage:" << src_size * 1.0 / compressed_data_size;
#endif
  return compressed_data_size;
}

size_t KafkaPkgHeadSize() {
  static size_t rs = 0;

  if (0 == rs) {
    KafkaPkg *pkg = new KafkaPkg;
    assert(NULL != pkg);

    pkg->set_split_flag(INT_MAX);
    pkg->set_seq_no(ULLONG_MAX);
    pkg->set_checksum(LLONG_MAX);
    // KafkaPkg data is required, so set data
    pkg->set_data(string(""));

    string pkg_str;
    pkg->SerializeToString(&pkg_str);

    delete pkg;

    rs = pkg_str.length();
  }

  return rs;
}

static uint64_t GetEpoch(void) {
  static uint64_t epoch = rdp_comm::Singleton<syncer::CheckPoint>::GetInstance().GetEpoch();
  return epoch;
}

SeqKafkaPkg::SeqKafkaPkg(const uint64_t split_seq_no_p, PkgSplitFlag split_flag_p, char *data,
                         const size_t data_size, struct SeqTrans *trans_p)
    : split_seq_no(split_seq_no_p), split_flag(split_flag_p), trans(trans_p), offset(0) {
  assert(NULL != trans);
  assert(NULL != data);
  buffer = (char *)malloc(data_size * sizeof(char));
  assert(NULL != buffer);
  memcpy(buffer, data, data_size * sizeof(char));
  buffer_size = data_size;
}

SeqKafkaPkg::~SeqKafkaPkg() {
  if (NULL != buffer) {
    free(buffer);
  }
}

// Get Key for kafka producer waiting for respnse map
string SeqKafkaPkg::Key(void) {
  char key[64] = {0x0};
  snprintf(key, sizeof(key) - 1, "%" PRIu64 "-%" PRIu64, trans->trans_seq_no, split_seq_no);
  return string(key);
}

SeqTransCtx::SeqTransCtx(const size_t p_split_msg_bytes, const bool p_enable_checksum,
                         const bool p_enable_compress, const bool p_enable_split_msg, 
                         const bool p_enable_trans_clean, const size_t default_buf_size)
    : split_msg_bytes(p_split_msg_bytes),
      enable_checksum(p_enable_checksum),
      enable_compress(p_enable_compress),
      enable_trans_clean(p_enable_trans_clean),
      enable_split_msg(p_enable_split_msg),
      transaction_buf(NULL),
      transaction_buf_size(0),
      compress_buf(NULL),
      compress_buf_size(0),
      dest_buf(NULL),
      dest_buf_size(0) {
  AllocTranBuf(default_buf_size);
  AllocCompressBuf(default_buf_size);
  AllocDestBuf(default_buf_size);
  kafka_pkg_head_size = KafkaPkgHeadSize();
}

SeqTransCtx::~SeqTransCtx() {
  if (transaction_buf)
    FREE_BUF(transaction_buf);
  if (compress_buf)
    FREE_BUF(compress_buf);
  if (dest_buf)
    FREE_BUF(dest_buf);
}

void SeqTransCtx::AllocTranBuf(const size_t need_size) {
  ALLOC_BUF(transaction_buf, need_size, transaction_buf_size);
}

void SeqTransCtx::AllocCompressBuf(const size_t need_size) {
  ALLOC_BUF(compress_buf, need_size, compress_buf_size);
}

void SeqTransCtx::AllocDestBuf(const size_t need_size) {
  ALLOC_BUF(dest_buf, need_size, dest_buf_size);
}

bool SeqTransCtx::IsLESplitMsgSize(size_t s) {
  RDP_LOG_DBG << "head size: " << kafka_pkg_head_size << " msg size: " << s << " max split msg size: " << split_msg_bytes;
  return kafka_pkg_head_size + s <= split_msg_bytes ? true : false;
}

static size_t TransactionSerialize(Transaction *transaction, SeqTransCtx *seq_trans_ctx) {
  size_t trans_serialize_size = transaction->ByteSizeLong();
  // 确保transaction_buf足够记录transaction序列化后数据
  seq_trans_ctx->AllocTranBuf(trans_serialize_size);

  // transaction序列化
  if (!transaction->SerializeToArray(seq_trans_ctx->transaction_buf, seq_trans_ctx->transaction_buf_size)) {
    RDP_LOG_ERROR << "Transaction Seq No: " << transaction->seq()
                  << ", GTID: " << transaction->gtid() << ", SerializeToString Failt";
    return 0;
  }

  return trans_serialize_size;
}

static size_t KfkPkgSerialize(char *data, size_t data_size, uint64_t trans_seq_no,
                              const string &gtid, int64_t checksum, uint64_t split_seq_no,
                              PkgSplitFlag split_flag, SeqTransCtx *seq_trans_ctx, uint64_t source_data_len) {
  KafkaPkg *pkg = new KafkaPkg;
  assert(NULL != pkg);

  pkg->set_version(rdp::messages::kPBVersion_1);

  pkg->set_trans_seq_no(trans_seq_no);
  pkg->set_epoch(GetEpoch());
  pkg->set_gtid(gtid);
  pkg->set_split_flag(split_flag);
  pkg->set_seq_no(split_seq_no);
  pkg->set_data(data, data_size);
  pkg->set_source_data_len(source_data_len);

  pkg->set_checksum(checksum);

  uint32_t pkg_flag = rdp::messages::kKfkPkgDefault;
  // 设置标识data字段为压缩后数据
  if (seq_trans_ctx->enable_compress) {
    pkg_flag |= rdp::messages::kKfkPkgCompressData;
  }
  pkg->set_flag(pkg_flag);

  size_t pkg_serialize_len = pkg->ByteSizeLong();
  // 分配足够空间存储序列化后数据
  seq_trans_ctx->AllocDestBuf(pkg_serialize_len);

  // Serialize序列化
  if (!pkg->SerializeToArray(seq_trans_ctx->dest_buf, seq_trans_ctx->dest_buf_size)) {
    RDP_LOG_ERROR << "Split Package Serialize KafkaPkg GTID:" << pkg->gtid()
                  << ", Seq No:" << pkg->seq_no() << " Failt";
    delete pkg;
    return 0;
  }

  delete pkg;
  return pkg_serialize_len;
}

static size_t VmsMsgSerialize(const string &msg_id, char *data, size_t data_size, SeqTransCtx *seq_trans_ctx) {
  VMSMessage *vms_msg = new VMSMessage;
  assert(NULL != vms_msg);

  vms_msg->set_messageid(msg_id);
  vms_msg->set_payload(data, data_size);

  size_t vms_msg_serialize_len = vms_msg->ByteSizeLong();
  // 分配足够空间存储序列化后数据
  seq_trans_ctx->AllocDestBuf(vms_msg_serialize_len);

  // VMSMessage序列化
  if (!vms_msg->SerializeToArray(seq_trans_ctx->dest_buf, seq_trans_ctx->dest_buf_size)) {
    RDP_LOG_ERROR << "Split Package Serialize VMSMessage Failt";
    delete vms_msg;
    return 0;
  }

  delete vms_msg;
  return vms_msg_serialize_len;
}

static char *KfkPkgAndVmsMsgSeria(SeqTransCtx *seq_trans_ctx, char *trans_buf, size_t trans_buf_len,
                                  uint64_t trans_seq_no, const string &gtid, int64_t checksum,
                                  uint64_t split_seq_no, PkgSplitFlag split_flag,
                                  size_t *serialize_len, uint64_t source_data_len) {
  assert(NULL != serialize_len);
  assert(NULL != seq_trans_ctx);
  assert(NULL != trans_buf);

  char *vms_msg_data = NULL;
  size_t vms_msg_data_size = 0;
  string vms_msg_id;

  *serialize_len = 0;

  RDP_LOG_DBG << "Split Transaction Seq No: " << trans_seq_no << ", GTID: " << gtid
              << ", Split Flag:" << split_flag << ", Split Seq No:" << split_seq_no
              << ", Data Len:" << trans_buf_len << ", Checksum:" << checksum;

  // 组装KafkaPkg并序列化
  size_t kfk_pkg_size =
      KfkPkgSerialize(trans_buf, trans_buf_len, trans_seq_no, gtid, checksum, split_seq_no,
                      split_flag, seq_trans_ctx, source_data_len);
  if (kfk_pkg_size <= 0) {
    RDP_LOG_ERROR << "Serialize KafkaPkg Failt";
    goto err;
  }

  vms_msg_data_size = kfk_pkg_size;
  vms_msg_data = seq_trans_ctx->dest_buf;

  vms_msg_id.append(boost::lexical_cast<string>(GetEpoch()));
  vms_msg_id.append(".");
  vms_msg_id.append(boost::lexical_cast<string>(trans_seq_no));
  vms_msg_id.append(".");
  vms_msg_id.append(boost::lexical_cast<string>(split_seq_no));

  // 组装VMSMessage并序列化
  if ((*serialize_len = VmsMsgSerialize(vms_msg_id, vms_msg_data, vms_msg_data_size, seq_trans_ctx)) <= 0) {
    RDP_LOG_ERROR << "Serialize VMSMessage Failt";
    goto err;
  }

  return seq_trans_ctx->dest_buf;

err:
  *serialize_len = 0;
  return NULL;
}

int ToSerialize(Transaction *transaction, SeqTransCtx *seq_trans_ctx,
             char *&serialize_data, size_t &serialize_data_len) {
  serialize_data = NULL;
  serialize_data_len = 0;
  size_t trans_serialize_size = 0;

  // transaction序列化
  if ((trans_serialize_size = TransactionSerialize(transaction, seq_trans_ctx)) <= 0) {
    RDP_LOG_ERROR << "Transaction Seq No: " << transaction->seq()
                  << ", GTID: " << transaction->gtid() << ", TransactionSerialize Failt";
    goto err;
  }

  if (seq_trans_ctx->enable_compress) {
    if ((serialize_data_len =
             Compress(seq_trans_ctx->transaction_buf, trans_serialize_size, seq_trans_ctx)) <= 0) {
      RDP_LOG_ERROR << "Compress Return:" << serialize_data_len << " Failt";
      goto err;
    }
    serialize_data = seq_trans_ctx->compress_buf;
  } else {
    serialize_data = seq_trans_ctx->transaction_buf;
    serialize_data_len = trans_serialize_size;
  }

  return 0;

err:
  return -1;
}

void SplitTransByEvent(Transaction *trans, list<Transaction*> &trans_list) {
  int source_event_size = trans->events_size();
  int first_event_size = source_event_size/2;

  Transaction *first_trans = new Transaction();
  assert(first_trans != NULL);

  first_trans->CopyFrom(*trans);

  ::google::protobuf::RepeatedPtrField< ::rdp::messages::Event >* first_events = first_trans->mutable_events();
  ::google::protobuf::RepeatedPtrField< ::rdp::messages::Event >::const_iterator first_clean_begin_it = first_events->begin() + first_event_size;
  ::google::protobuf::RepeatedPtrField< ::rdp::messages::Event >::const_iterator first_clean_end_it = first_events->end();
  first_events->erase(first_clean_begin_it, first_clean_end_it);

  Transaction *second_trans = new Transaction();
  assert(second_trans != NULL);

  second_trans->CopyFrom(*trans);

  ::google::protobuf::RepeatedPtrField< ::rdp::messages::Event >* second_events = second_trans->mutable_events();
  ::google::protobuf::RepeatedPtrField< ::rdp::messages::Event >::const_iterator second_clean_begin_it = second_events->begin();
  ::google::protobuf::RepeatedPtrField< ::rdp::messages::Event >::const_iterator second_clean_end_it = second_clean_begin_it + first_event_size;
  second_events->erase(second_clean_begin_it, second_clean_end_it);

  trans_list.push_front(second_trans);
  trans_list.push_front(first_trans);
  
  RDP_LOG_DBG << "split transaction seq no:" << trans->seq() << 
                 " by events, total events:" << trans->events_size() << 
                 " first events size:" << first_trans->events_size() << 
                 " second events size:" << second_trans->events_size();
}

void SplitTransByRow(Transaction *trans, list<Transaction*> &trans_list) {
  assert(trans != NULL);
  assert(trans->events_size() == 1);

  int source_row_size = trans->events(0).rows_size();
  int first_row_size = source_row_size/2;

  Transaction *first_trans = new Transaction();
  assert(first_trans != NULL);

  first_trans->CopyFrom(*trans);

  ::google::protobuf::RepeatedPtrField< ::rdp::messages::Row >* first_rows = first_trans->mutable_events(0)->mutable_rows();
  ::google::protobuf::RepeatedPtrField< ::rdp::messages::Row >::const_iterator first_clean_begin_it = first_rows->begin() + first_row_size;
  ::google::protobuf::RepeatedPtrField< ::rdp::messages::Row >::const_iterator first_clean_end_it = first_rows->end();
  first_rows->erase(first_clean_begin_it, first_clean_end_it);

  Transaction *second_trans = new Transaction();
  assert(second_trans != NULL);

  second_trans->CopyFrom(*trans);

  ::google::protobuf::RepeatedPtrField< ::rdp::messages::Row >* second_rows = second_trans->mutable_events(0)->mutable_rows();
  ::google::protobuf::RepeatedPtrField< ::rdp::messages::Row >::const_iterator second_clean_begin_it = second_rows->begin();
  ::google::protobuf::RepeatedPtrField< ::rdp::messages::Row >::const_iterator second_clean_end_it = second_clean_begin_it + first_row_size;
  second_rows->erase(second_clean_begin_it, second_clean_end_it);

  trans_list.push_front(second_trans);
  trans_list.push_front(first_trans);
  
  RDP_LOG_DBG << "split transaction seq no:" << trans->seq() << 
                 " by rows, total rows:" << trans->events(0).rows_size() << 
                 " first events size:" << first_trans->events(0).rows_size() << 
                 " second events size:" << second_trans->events(0).rows_size();

}

void SplitTransaction(Transaction *trans, list<Transaction*> &trans_list) {
  assert(trans != NULL);
  if (trans->events_size() > 1) {
    SplitTransByEvent(trans, trans_list);
  } else {
    SplitTransByRow(trans, trans_list);
  }
}

bool TransToSeqKafkaPkgAndSave(Transaction *trans, 
                               SeqTrans *seq_trans,
                               SeqTransCtx *seq_trans_ctx,
                               list<Transaction*> &trans_list,
                               char * transaction_data,
                               const size_t transaction_data_len) {
  assert(trans != NULL);
  assert(seq_trans != NULL);
  assert(seq_trans_ctx != NULL);

  char* serialize_data = NULL;
  size_t serialize_data_len = 0;
  int64_t checksum = -1;
  uint64_t split_seq_no = 0;
  PkgSplitFlag split_flag;

  // 计算checksum
  if (seq_trans_ctx->enable_checksum)
    checksum = CheckSum::Get(transaction_data, transaction_data_len);

  // 如果trans_list为空，且seq_trans->pkgs为空，则说明这个事务没有分包
  if (trans_list.empty() && seq_trans->pkgs.empty()) {
    split_flag = PKG_NOT_SPLIT;
  } else {
    // 如果trans_list为空，seq_trans->pkgs为不空，则说明这个事务分包结束
    if (trans_list.empty() && !seq_trans->pkgs.empty())
      split_flag = PKG_SPLIT_END;
    else
      split_flag = PKG_SPLIT;
  }

  // 分包是按照次序依次加入seq_trans->pkgs的，所以seq_trans->pkgs.size()
  split_seq_no = seq_trans->pkgs.size();

  // 将分切数据先打包为KafkaPkg和序列化，再打包为VMSMessage和序列化，返回最终发送到kafka的数据
  serialize_data = KfkPkgAndVmsMsgSeria(seq_trans_ctx, 
                                        transaction_data, 
                                        transaction_data_len, 
                                        // trans->seq()为32位，容易越界，不能用
                                        seq_trans->trans_seq_no,
                                        trans->gtid(),
                                        checksum, 
                                        split_seq_no, 
                                        split_flag, 
                                        &serialize_data_len,
                                        trans->ByteSizeLong());
  if (NULL == serialize_data) {
    RDP_LOG_ERROR << "KfkPkgAndVmsMsgSeria Seq: " << seq_trans->trans_seq_no << ", GTID:" << trans->gtid() << " Failt";
    return false;
  }

  // 动态分配内存存储serialize_data
  SeqKafkaPkg *seq_kfk_pkg = new SeqKafkaPkg(split_seq_no, split_flag, serialize_data, serialize_data_len, seq_trans);
  assert(NULL != seq_kfk_pkg);

  // 保存每个分包
  seq_trans->pkgs.push_back(seq_kfk_pkg);

  RDP_LOG_DBG << "Split Transaction Seq No: " << seq_trans->trans_seq_no << ", GTID: " << trans->gtid()
    << " To Pkg Index: " << split_seq_no << ", Split Flag: " << split_flag
    << ", Data Len: " << serialize_data_len;

  return true;
}

inline bool IsOneEventRow(Transaction *trans) {
  assert(trans != NULL);
  if (trans->events_size() > 1)
    return false;
  assert(trans->events_size() == 1);
  if (trans->events(0).rows_size() > 1)
    return false;
  // if events is "is_truncated" rows_size() == 0
  assert(trans->events(0).rows_size() <= 1);
  return true;
}

void CleanTransRowsAndAlarm(Transaction *trans) {
  assert(trans != NULL);
  assert(trans->events_size() == 1);

  // event 没有数据，不需要告警
  if (trans->events(0).rows_size() <= 0) {
    trans->clear_events();
    return;
  }

  string output;
  for (int i = 0; i < trans->events(0).rows(0).before_size(); ++i) {
    if (i != 0) {
      output.append("|");
    }
    const ::rdp::messages::Column& column = trans->events(0).rows(0).before(i);
    output.append(column.name());
    output.append(":");
    output.append(column.type());
    output.append(":");
    output.append(column.value());
  }
  output.append("\n");

  for (int i = 0; i < trans->events(0).rows(0).after_size(); ++i) {
    if (i != 0) {
      output.append("|");
    }
    const ::rdp::messages::Column& column = trans->events(0).rows(0).after(i);
    output.append(column.name());
    output.append(":");
    output.append(column.type());
    output.append(":");
    output.append(column.value());
  }
  output.append("\n");

  RDP_LOG_WARN << "clean transaction gtid: " << trans->gtid() << 
                  " seq no: " << trans->seq() << 
                  " binlog file name: " << trans->binlog_file_name() <<
                  " position: " << trans->position() <<
                  " event position: " << trans->events(0).position() << 
                  " event database: " << trans->events(0).database_name() << 
                  " event table: " << trans->events(0).table_name() <<
                  " event type: " << trans->events(0).event_type() << std::endl <<
                  output; 

  output.clear();

  output.append("GTID:");
  output.append(trans->gtid());
  output.append(" Binlog:");
  output.append(trans->binlog_file_name());
  output.append(":");
  output.append(boost::lexical_cast<string>(trans->position()));
  output.append(" event position:");
  output.append(boost::lexical_cast<string>(trans->events(0).position()));
  output.append(" db:");
  output.append(trans->events(0).database_name());
  output.append(" table:");
  output.append(trans->events(0).table_name());

  g_syncer_metric->filte_trx_count_metric_->AddValue(1);
  g_syncer_app->Alarm(output); 

  trans->mutable_events(0)->clear_rows();
}

int ProcessTransaction(Transaction *trans,
                       SeqTrans *seq_trans,
                       SeqTransCtx *seq_trans_ctx,
                       list<Transaction*> &trans_list) {
  char *serialize_data = NULL;
  size_t serialize_data_len = 0;

  RDP_LOG_DBG << "transaction seq no:" << seq_trans->trans_seq_no << 
                 " size:" << trans->ByteSizeLong() << 
                 " split msg size:" << seq_trans_ctx->split_msg_bytes << 
                 " kafka pkg head:" << seq_trans_ctx->kafka_pkg_head_size;

  if (0 != ToSerialize(trans, seq_trans_ctx, serialize_data, serialize_data_len)) {
    RDP_LOG_ERROR << "ToSerialize error";
    return -1;
  }

  // 超出Kafka消息大小
  if (!seq_trans_ctx->IsLESplitMsgSize(serialize_data_len)) {
    if (IsOneEventRow(trans)) {
      // 如果事务只有一个event一个row，大小还是超过Kafka消息大小，则清除row中数据，并且告警
      CleanTransRowsAndAlarm(trans);
      return 1;
    }
    // 分包
    SplitTransaction(trans, trans_list);
    return 0;
  }

  if (!TransToSeqKafkaPkgAndSave(trans, seq_trans, seq_trans_ctx, trans_list, serialize_data, serialize_data_len)) {
    RDP_LOG_ERROR << "TransToSeqKafkaPkgAndSave error";
    return -1;
  }

  return 0;
}

//   ----------> trans_list.front <---------------------
//   |           trans_list.pop_front                  |
//   |                     |                           |
//   |                序列化&压缩                      |
//   |                     |                           |
//   |            是否超出Kafka消息大小                |
//   |                    / \                          |
//   |                  N/   \Y                        |
//   |                  /     \                        |
//   |                 /    是否只有一个Event一个Row   |
//   |                /              /\                |
//   |     ----------/             Y/  \N              |
//   |     |                       /    \              |
//   |     |                  清空trans  切分事务      |
//   |     |                  并告警      /            |
//   |    KfkPkg打包              \      /             |
//   |     |                       \    /              |
//   |     |                        \  /               |
//   ---加入seq_trans->pkgs   trans_list.push_front-----

bool ProcessTransactionList(list<Transaction*> &trans_list,  
                            SeqTrans *seq_trans,  
                            SeqTransCtx *seq_trans_ctx) {
  int ret = 0;
  // 已经处理完成
  if (trans_list.empty()) return true;

  Transaction *trans = NULL;

  while (!trans_list.empty()) {
    trans = trans_list.front();
    trans_list.pop_front();
    assert(trans != NULL);

    RDP_LOG_DBG << "pop transaction gtid:" << trans->gtid() << " seq no:" << seq_trans->trans_seq_no << " event size:" << trans->events_size();

    ret = ProcessTransaction(trans, seq_trans, seq_trans_ctx, trans_list);
    switch (ret) {
      // 返回异常
      case -1:
        delete trans;
        abort();
        break;
      // 数据被过滤
      case 1:
        trans_list.push_front(trans);
        break;
      // success
      case 0:
        delete trans;
        trans = NULL;
        break;
      default:
        abort();
    }
  }
  return true;
} 

static void CopyTrxFilePosInf(Transaction *dest, Transaction *src) {
  dest->set_gtid(src->gtid());
  dest->set_seq(src->seq());
  dest->set_position(src->position());
  dest->set_binlog_file_name(src->binlog_file_name());
  dest->set_next_position(src->next_position());
  dest->set_next_binlog_file_name(src->next_binlog_file_name());
}

static SeqTrans *DisableSplitMsgProcess(Transaction *transaction,
                                        SeqTransCtx *seq_trans_ctx,
                                        uint64_t trans_seq_no) {
  char *serialize_data = NULL;
  size_t serialize_data_len = 0;
  list<Transaction*> trans_list;

  // transaction->seq()为32位，不能使用
  SeqTrans *seq_trans = new SeqTrans(trans_seq_no, transaction->gtid(), GetEpoch());
  assert(seq_trans != NULL);

  seq_trans->event_count = transaction->events_size();
  // transaction->events 不需要，所以不能直接赋值
  //seq_trans->transaction = *transaction;
  CopyTrxFilePosInf(&seq_trans->transaction, transaction);

  RDP_LOG_DBG << "transaction seq no:" << seq_trans->trans_seq_no << 
    " size:" << transaction->ByteSizeLong() << 
    " split msg size:" << seq_trans_ctx->split_msg_bytes << 
    " kafka pkg head:" << seq_trans_ctx->kafka_pkg_head_size;

  if (0 != ToSerialize(transaction, seq_trans_ctx, serialize_data, serialize_data_len)) {
    RDP_LOG_ERROR << "ToSerialize error";
    goto err;
  }
  // 判断是否超出Kafka消息大小
  if (!seq_trans_ctx->IsLESplitMsgSize(serialize_data_len)) {

    RDP_LOG_WARN << "Before clear rows, the trx size is: " << transaction->ByteSizeLong();

    // 清空insert/update/delete event内的rows
    ClearRowsInDataEvents(transaction);

    RDP_LOG_WARN << "After clear rows, the trx size is: " << transaction->ByteSizeLong();
    // 重新序列化&压缩
    if (0 != ToSerialize(transaction, seq_trans_ctx, serialize_data, serialize_data_len)) {
      RDP_LOG_ERROR << "ToSerialize error";
      goto err;
    }
    // 再次判断是否超出Kafka消息大小
    if (!seq_trans_ctx->IsLESplitMsgSize(serialize_data_len)) {

      RDP_LOG_WARN << "Before clear events, the trx size is: " << transaction->ByteSizeLong() + seq_trans_ctx->kafka_pkg_head_size;
      // 清空所有events
      transaction->clear_events();
      RDP_LOG_WARN << "After clear events, the trx size is: " << transaction->ByteSizeLong() + seq_trans_ctx->kafka_pkg_head_size;
      // 重新序列化&压缩
      if (0 != ToSerialize(transaction, seq_trans_ctx, serialize_data, serialize_data_len)) {
        RDP_LOG_ERROR << "ToSerialize error";
        goto err;
      }
      // 现在只有事务头信息，如果还是超出Kafka消息大小，说明Kafka消息设置太小，报错退出
      if (!seq_trans_ctx->IsLESplitMsgSize(serialize_data_len)) {
        RDP_LOG_ERROR << "split_msg_bytes: " << seq_trans_ctx->split_msg_bytes << " is too small error";
        goto err;
      }
    }
  }

  if (!TransToSeqKafkaPkgAndSave(transaction, seq_trans, seq_trans_ctx, trans_list, serialize_data, serialize_data_len)) {
    RDP_LOG_ERROR << "TransToSeqKafkaPkgAndSave error";
    goto err;
  }

  return seq_trans;

err:

  delete seq_trans;
  return NULL;
}

static SeqTrans *EnableSplitMsgProcess(Transaction *transaction,
                                       SeqTransCtx *seq_trans_ctx,
                                       uint64_t trans_seq_no) {
  assert(NULL != transaction);
  assert(NULL != seq_trans_ctx);
  assert(true == seq_trans_ctx->enable_split_msg);

  list<Transaction*> trans_list;
  trans_list.push_back(new Transaction(*transaction));

  // transaction->seq()为32位，不能使用
  SeqTrans *seq_trans = new SeqTrans(trans_seq_no, transaction->gtid(), GetEpoch());
  assert(seq_trans != NULL);

  seq_trans->event_count = transaction->events_size();
  // transaction->events 不需要，所以不能直接赋值
  //seq_trans->transaction = *transaction;
  CopyTrxFilePosInf(&seq_trans->transaction, transaction);

  if (!ProcessTransactionList(trans_list, seq_trans, seq_trans_ctx)) {
    goto err;
  }

  assert(trans_list.empty() == true);

  return seq_trans;

err:
  Transaction *trans_tmp = NULL;
  while (!trans_list.empty()) {
    trans_tmp = trans_list.front();
    trans_list.pop_front();
    assert(trans_tmp != NULL);
    delete trans_tmp;
  }
  delete seq_trans;
  return NULL;

}

SeqTrans *TransactionToSeqTrans(Transaction *transaction,
                                SeqTransCtx *seq_trans_ctx,
                                uint64_t trans_seq_no) {
  assert(NULL != transaction);
  assert(NULL != seq_trans_ctx);


  if (seq_trans_ctx->enable_split_msg) {
    return EnableSplitMsgProcess(transaction, seq_trans_ctx, trans_seq_no);
  } else {
    return DisableSplitMsgProcess(transaction, seq_trans_ctx, trans_seq_no);
  }
}


void ClearRowsInDataEvents(rdp::messages::Transaction *transaction) {
  // 不会存在没有ClearRowsInDataEvents而进行clear_events的事务
  // 告警
  ClearTransEventAlarm(transaction);
  for (int i = 0; i < transaction->events_size(); i++) {
    rdp::messages::Event *event = transaction->mutable_events(i);
    if (IsDataEvent(event->event_type())) event->clear_rows();
  }
  return;
}

// 判断进行clear event的transaction中是否存在insert/update/delete的数据,如果有才进行告警
// 如果只操作一个database和一个table，则不会带上'...'
// 如果操作多个database或者table，则会带上'...'
void ClearTransEventAlarm(rdp::messages::Transaction *transaction) {
  string first_database;
  string first_table;
  string alarm_str;
  bool mutdb_table_flag = false;

  for (int i = 0; i < transaction->events_size(); i++) {
    const rdp::messages::Event event = transaction->events(i);
    /*
      mysql 5.7.17
      WRITE_ROWS_EVENT = 30, 对应insert操作
      UPDATE_ROWS_EVENT = 31, 对应update操作
      DELETE_ROWS_EVENT = 32, 对应delete/update操作
    */
    if (IsDataEvent(event.event_type()) && 
        !g_syncer_app->syncer_filter_->IsIgnored(event.database_name(), event.table_name())) {
      if (first_database.empty() || first_table.empty()) {
        first_database = event.database_name();
        first_table = event.table_name();
      } else {
        if (first_database.compare(event.database_name()) != 0 || first_table.compare(event.table_name()) != 0)
          mutdb_table_flag = true;
      }
    }
  }

  if (!first_database.empty() || !first_table.empty()) {
    uint64_t trans_len = transaction->next_position() - transaction->position();
    alarm_str.append("GTID:");
    alarm_str.append(transaction->gtid());
    alarm_str.append(" Binlog:");
    alarm_str.append(transaction->binlog_file_name());
    alarm_str.append(":");
    alarm_str.append(boost::lexical_cast<string>(transaction->position()));
    alarm_str.append("~");
    alarm_str.append(boost::lexical_cast<string>(transaction->next_position()));
    alarm_str.append(" Len:");
    alarm_str.append(boost::lexical_cast<string>(trans_len));
    alarm_str.append(" DB:");
    alarm_str.append(first_database);
    alarm_str.append(" Table:");
    alarm_str.append(first_table);
    if (mutdb_table_flag)
      alarm_str.append(" ...");
    // 监控
    g_syncer_metric->filte_trx_count_metric_->AddValue(1);

    RDP_LOG_WARN << "Filte Transaction: " << alarm_str;
    g_syncer_app->Alarm(alarm_str);
  }
}



}  // namespace syncer
