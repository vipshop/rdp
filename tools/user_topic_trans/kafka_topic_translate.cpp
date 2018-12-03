/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2014, Magnus Edenhill
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

/**
 * Apache Kafka consumer & producer example programs
 * using the Kafka driver from librdkafka
 * (https://github.com/edenhill/librdkafka)
 */

#include <iostream>
#include <string>
#include <cstdlib>
#include <cstdio>
#include <csignal>
#include <cstring>
#include <vector>

#include <rdp.pb.h>

#include <getopt.h>
#include <lz4.h>

/*
 * Typically include path in a real application would be
 * #include <librdkafka/rdkafkacpp.h>
 */
#include <rdkafkacpp.h>

using std::vector;
using std::string;

#define ALLOC_BUF(buf, need_size, buf_size) \
  do { \
    if ((buf) == NULL) { \
      if ((buf_size) < (need_size)) \
        (buf_size) = (need_size); \
      (buf) = (char*)malloc((buf_size)); \
      assert((buf) != NULL); \
      break; \
    } \
    if ((need_size) > (buf_size)) { \
      (buf) = (char *)realloc((buf), (need_size)); \
      assert(NULL != (buf)); \
      (buf_size) = (need_size); \
    } \
  } while(0);

#define FREE_BUF(buf) \
  do { \
    if ((buf) != NULL) { \
      free(buf); \
      (buf) = NULL; \
    } \
  } while(0);


/**
  @return
    returns the human readable name of the event's type
    */

/**
  Enumeration type for the different types of log events.
  */
#if 0
enum Log_event_type
{
  /**
    Every time you update this enum (when you add a type), you have to
    fix Format_description_event::Format_description_event().
    */
  UNKNOWN_EVENT = 0,
  START_EVENT_V3 = 1,
  QUERY_EVENT = 2,
  STOP_EVENT = 3,
  ROTATE_EVENT = 4,
  INTVAR_EVENT = 5,
  LOAD_EVENT = 6,
  SLAVE_EVENT = 7,
  CREATE_FILE_EVENT = 8,
  APPEND_BLOCK_EVENT = 9,
  EXEC_LOAD_EVENT = 10,
  DELETE_FILE_EVENT = 11,
  /**
    NEW_LOAD_EVENT is like LOAD_EVENT except that it has a longer
    sql_ex, allowing multibyte TERMINATED BY etc; both types share the
    same class (Load_event)
    */
  NEW_LOAD_EVENT = 12,
  RAND_EVENT = 13,
  USER_VAR_EVENT = 14,
  FORMAT_DESCRIPTION_EVENT = 15,
  XID_EVENT = 16,
  BEGIN_LOAD_QUERY_EVENT = 17,
  EXECUTE_LOAD_QUERY_EVENT = 18,
  TABLE_MAP_EVENT = 19,

  /**
    The PRE_GA event numbers were used for 5.1.0 to 5.1.15 and are
    therefore obsolete.
    */
  PRE_GA_WRITE_ROWS_EVENT = 20,
  PRE_GA_UPDATE_ROWS_EVENT = 21,
  PRE_GA_DELETE_ROWS_EVENT = 22,

  /**
    The V1 event numbers are used from 5.1.16 until mysql-trunk-xx
    */
  WRITE_ROWS_EVENT_V1 = 23,
  UPDATE_ROWS_EVENT_V1 = 24,
  DELETE_ROWS_EVENT_V1 = 25,

  /**
    Something out of the ordinary happened on the master
    */
  INCIDENT_EVENT = 26,

  /**
    Heartbeat event to be send by master at its idle time
    to ensure master's online status to slave
    */
  HEARTBEAT_LOG_EVENT = 27,

  /**
    In some situations, it is necessary to send over ignorable
    data to the slave: data that a slave can handle in case there
    is code for handling it, but which can be ignored if it is not
    recognized.
    */
  IGNORABLE_LOG_EVENT = 28,
  ROWS_QUERY_LOG_EVENT = 29,

  /** Version 2 of the Row events */
  WRITE_ROWS_EVENT = 30,
  UPDATE_ROWS_EVENT = 31,
  DELETE_ROWS_EVENT = 32,
  GTID_LOG_EVENT = 33,
  ANONYMOUS_GTID_LOG_EVENT = 34,
  PREVIOUS_GTIDS_LOG_EVENT = 35,
  TRANSACTION_CONTEXT_EVENT = 36,
  VIEW_CHANGE_EVENT = 37,

  /* Prepared XA transaction terminal event similar to Xid */
  XA_PREPARE_LOG_EVENT = 38,

  /**
    Add new events here - right above this comment!
    Existing events (except ENUM_END_EVENT) should never change their numbers
    */
  ENUM_END_EVENT /* end marker */
};
#else
enum Log_event_type
{
  /*
   *     Every time you update this enum (when you add a type), you have to
   *         fix Format_description_log_event::Format_description_log_event().
   *           */
  UNKNOWN_EVENT= 0,
  START_EVENT_V3= 1,
  QUERY_EVENT= 2,
  STOP_EVENT= 3,
  ROTATE_EVENT= 4,
  INTVAR_EVENT= 5,
  LOAD_EVENT= 6,
  SLAVE_EVENT= 7,
  CREATE_FILE_EVENT= 8,
  APPEND_BLOCK_EVENT= 9,
  EXEC_LOAD_EVENT= 10,
  DELETE_FILE_EVENT= 11,
  /*
   *     NEW_LOAD_EVENT is like LOAD_EVENT except that it has a longer
   *         sql_ex, allowing multibyte TERMINATED BY etc; both types share the
   *             same class (Load_log_event)
   *               */
  NEW_LOAD_EVENT= 12,
  RAND_EVENT= 13,
  USER_VAR_EVENT= 14,
  FORMAT_DESCRIPTION_EVENT= 15,
  XID_EVENT= 16,
  BEGIN_LOAD_QUERY_EVENT= 17,
  EXECUTE_LOAD_QUERY_EVENT= 18,

  TABLE_MAP_EVENT = 19,

  /*
   *     These event numbers were used for 5.1.0 to 5.1.15 and are
   *         therefore obsolete.
   *            */
  PRE_GA_WRITE_ROWS_EVENT = 20,
  PRE_GA_UPDATE_ROWS_EVENT = 21,
  PRE_GA_DELETE_ROWS_EVENT = 22,

  /*
   *     These event numbers are used from 5.1.16 until mysql-5.6.6,
   *         and in MariaDB
   *            */
  WRITE_ROWS_EVENT_V1 = 23,
  UPDATE_ROWS_EVENT_V1 = 24,
  DELETE_ROWS_EVENT_V1 = 25,

  /*
   *     Something out of the ordinary happened on the master
   *        */
  INCIDENT_EVENT= 26,

  /*
   *     Heartbeat event to be send by master at its idle time 
   *         to ensure master's online status to slave 
   *           */
  HEARTBEAT_LOG_EVENT= 27,

  /*
   *     In some situations, it is necessary to send over ignorable
   *         data to the slave: data that a slave can handle in case there
   *             is code for handling it, but which can be ignored if it is not
   *                 recognized.
   *
   *                     These mysql-5.6 events are not recognized (and ignored) by MariaDB
   *                       */
  IGNORABLE_LOG_EVENT= 28,
  ROWS_QUERY_LOG_EVENT= 29,

  /* Version 2 of the Row events, generated only by mysql-5.6.6+ */
  WRITE_ROWS_EVENT = 30,
  UPDATE_ROWS_EVENT = 31,
  DELETE_ROWS_EVENT = 32,

  /*
   *     Add new events here - right above this comment!
   *         Existing events (except ENUM_END_EVENT) should never change their numbers
   *           */

  /* New MySQL/Sun events are to be added right above this comment */
  MYSQL_EVENTS_END,

  MARIA_EVENTS_BEGIN= 160,
  /* New Maria event numbers start from here */
  ANNOTATE_ROWS_EVENT= 160,
  /*
   *     Binlog checkpoint event. Used for XA crash recovery on the master, not used
   *         in replication.
   *             A binlog checkpoint event specifies a binlog file such that XA crash
   *                 recovery can start from that file - and it is guaranteed to find all XIDs
   *                     that are prepared in storage engines but not yet committed.
   *                       */
  BINLOG_CHECKPOINT_EVENT= 161,
  /*
   *     Gtid event. For global transaction ID, used to start a new event group,
   *         instead of the old BEGIN query event, and also to mark stand-alone
   *             events.
   *               */
  GTID_EVENT= 162,
  /*
   *     Gtid list event. Logged at the start of every binlog, to record the
   *         current replication state. This consists of the last GTID seen for
   *             each replication domain.
   *               */
  GTID_LIST_EVENT= 163,

  /* Add new MariaDB events here - right above this comment!  */

  ENUM_END_EVENT /* end marker */
};
#endif

#if 0
const char* get_type_str(uint32_t type)
{
  switch(type) {
    case START_EVENT_V3:  return "Start_v3";
    case STOP_EVENT:   return "Stop";
    case QUERY_EVENT:  return "Query";
    case ROTATE_EVENT: return "Rotate";
    case INTVAR_EVENT: return "Intvar";
    case LOAD_EVENT:   return "Load";
    case NEW_LOAD_EVENT:   return "New_load";
    case CREATE_FILE_EVENT: return "Create_file";
    case APPEND_BLOCK_EVENT: return "Append_block";
    case DELETE_FILE_EVENT: return "Delete_file";
    case EXEC_LOAD_EVENT: return "Exec_load";
    case RAND_EVENT: return "RAND";
    case XID_EVENT: return "Xid";
    case USER_VAR_EVENT: return "User var";
    case FORMAT_DESCRIPTION_EVENT: return "Format_desc";
    case TABLE_MAP_EVENT: return "Table_map";
    case PRE_GA_WRITE_ROWS_EVENT: return "Write_rows_event_old";
    case PRE_GA_UPDATE_ROWS_EVENT: return "Update_rows_event_old";
    case PRE_GA_DELETE_ROWS_EVENT: return "Delete_rows_event_old";
    case WRITE_ROWS_EVENT_V1: return "Write_rows_v1";
    case UPDATE_ROWS_EVENT_V1: return "Update_rows_v1";
    case DELETE_ROWS_EVENT_V1: return "Delete_rows_v1";
    case BEGIN_LOAD_QUERY_EVENT: return "Begin_load_query";
    case EXECUTE_LOAD_QUERY_EVENT: return "Execute_load_query";
    case INCIDENT_EVENT: return "Incident";
    case IGNORABLE_LOG_EVENT: return "Ignorable";
    case ROWS_QUERY_LOG_EVENT: return "Rows_query";
    case WRITE_ROWS_EVENT: return "Write_rows";
    case UPDATE_ROWS_EVENT: return "Update_rows";
    case DELETE_ROWS_EVENT: return "Delete_rows";
    case GTID_LOG_EVENT: return "Gtid";
    case ANONYMOUS_GTID_LOG_EVENT: return "Anonymous_Gtid";
    case PREVIOUS_GTIDS_LOG_EVENT: return "Previous_gtids";
    case HEARTBEAT_LOG_EVENT: return "Heartbeat";
    case TRANSACTION_CONTEXT_EVENT: return "Transaction_context";
    case VIEW_CHANGE_EVENT: return "View_change";
    case XA_PREPARE_LOG_EVENT: return "XA_prepare";
    default: return "Unknown";                            /* impossible */
  }
}
#else
const char* get_type_str(uint32_t type)
{
  switch(type) {
    case START_EVENT_V3:  return "Start_v3";
    case STOP_EVENT:   return "Stop";
    case QUERY_EVENT:  return "Query";
    case ROTATE_EVENT: return "Rotate";
    case INTVAR_EVENT: return "Intvar";
    case LOAD_EVENT:   return "Load";
    case NEW_LOAD_EVENT:   return "New_load";
    case SLAVE_EVENT:  return "Slave";
    case CREATE_FILE_EVENT: return "Create_file";
    case APPEND_BLOCK_EVENT: return "Append_block";
    case DELETE_FILE_EVENT: return "Delete_file";
    case EXEC_LOAD_EVENT: return "Exec_load";
    case RAND_EVENT: return "RAND";
    case XID_EVENT: return "Xid";
    case USER_VAR_EVENT: return "User var";
    case FORMAT_DESCRIPTION_EVENT: return "Format_desc";
    case TABLE_MAP_EVENT: return "Table_map";
    case PRE_GA_WRITE_ROWS_EVENT: return "Write_rows_event_old";
    case PRE_GA_UPDATE_ROWS_EVENT: return "Update_rows_event_old";
    case PRE_GA_DELETE_ROWS_EVENT: return "Delete_rows_event_old";
    //case WRITE_ROWS_EVENT_V1: return "Write_rows_v1";
    //case UPDATE_ROWS_EVENT_V1: return "Update_rows_v1";
    //case DELETE_ROWS_EVENT_V1: return "Delete_rows_v1";
    //case WRITE_ROWS_EVENT: return "Write_rows";
    //case UPDATE_ROWS_EVENT: return "Update_rows";
    //case DELETE_ROWS_EVENT: return "Delete_rows";
    case WRITE_ROWS_EVENT_V1: return "insert";
    case UPDATE_ROWS_EVENT_V1: return "update";
    case DELETE_ROWS_EVENT_V1: return "delete";
    case WRITE_ROWS_EVENT: return "insert";
    case UPDATE_ROWS_EVENT: return "update";
    case DELETE_ROWS_EVENT: return "delete";
    case BEGIN_LOAD_QUERY_EVENT: return "Begin_load_query";
    case EXECUTE_LOAD_QUERY_EVENT: return "Execute_load_query";
    case INCIDENT_EVENT: return "Incident";
    case ANNOTATE_ROWS_EVENT: return "Annotate_rows";
    case BINLOG_CHECKPOINT_EVENT: return "Binlog_checkpoint";
    case GTID_EVENT: return "Gtid";
    case GTID_LIST_EVENT: return "Gtid_list";
    default: return "Unknown";        /* impossible */
  }
}

#endif

static string g_schema_name;
static string g_table_name;
static vector<string> g_column_list;
static vector<int32_t> g_event_type_list;

static int64_t end_offset = RdKafka::Topic::OFFSET_END;
static int64_t read_len = -1;
static int64_t read_num = 0;
static std::string continue_read;
static int64_t message_read_index = 0;
static bool get_water = false;

static char *g_decompress_buf = NULL;
static size_t g_decompress_buf_size = 10 * 1024*1024;

static bool concise_print = false;

static int64_t g_offset = 0;

static int Decompress(const char *src, const size_t src_size, char **dst, size_t *dst_size) {
  const int max_dst_size = src_size * 10;
  ALLOC_BUF(*dst, max_dst_size, *dst_size);
  const int decompressed_data_size = LZ4_decompress_safe(src, *dst, src_size, *dst_size);
  if (decompressed_data_size < 0)
    std::cerr << "A negative result from LZ4_decompress_safe" 
      "indicates a failure trying to compress the data.  See exit code (echo " << decompressed_data_size << ") for value returned." << std::endl;
  if (decompressed_data_size == 0)
    std::cerr << "A result of 0 means compression worked, but was stopped because the destination buffer couldn't hold all the information." << std::endl;
  return decompressed_data_size;
}

static inline bool IsInsUpDelEvent(const int32_t event_type) {
  /*
     MariaDB 10.0.21
     WRITE_ROWS_EVENT_V1 = 23, 对应insert操作
     UPDATE_ROWS_EVENT_V1 = 24, 对应update操作
     DELETE_ROWS_EVENT_V1 = 25, 对应delete/update操作
     MySQL 5.7.17
     WRITE_ROWS_EVENT = 30,
     UPDATE_ROWS_EVENT = 31,
     DELETE_ROWS_EVENT = 32
     */
  if (event_type != WRITE_ROWS_EVENT_V1 && 
      event_type != UPDATE_ROWS_EVENT_V1 && 
      event_type != DELETE_ROWS_EVENT_V1 && 
      event_type != WRITE_ROWS_EVENT && 
      event_type != UPDATE_ROWS_EVENT && 
      event_type != DELETE_ROWS_EVENT) {
    return false;
  }
  return true;
}

static void SplitString(const string &src, vector<string> &dst, const string &c) {
  string::size_type start_pos = 0, end_pos;
  while (string::npos != (end_pos = src.find(c, start_pos))) {
    dst.push_back(src.substr(start_pos, end_pos - start_pos));
    start_pos = end_pos + c.length();
  }

  if (start_pos != src.length())
    dst.push_back(src.substr(start_pos));
}

static bool ToEventTypeList(const string &src) {
  if (src.empty()) return false;
  vector<string> tmp;
  SplitString(src, tmp, ",");
  for (vector<string>::iterator it = tmp.begin(); it != tmp.end(); it++) {
    int event_type = atoi(it->c_str());
    if (!IsInsUpDelEvent(event_type))
      return false;
    g_event_type_list.push_back(event_type);
  }
  return true;
}

static bool ToColumnList(const string &src) {
  if (src.empty()) return false;
  SplitString(src, g_column_list, ",");
  if (g_column_list.empty())
    return false;
  else
    return true;
}

static int CompareNoCase(const string &s1, const string &s2) {
  char *s1_ptr = const_cast<char*>(s1.c_str());
  char *s2_ptr = const_cast<char*>(s2.c_str());
  int min_len = s1.length() < s2.length() ? s1.length() : s2.length();
  for (int i = 0; i < min_len; ++i) {
    if (std::toupper(*(s1_ptr+i)) < std::toupper(*(s2_ptr+i))) {
      return -1;
    } else if (std::toupper(*(s1_ptr+i)) > std::toupper(*(s2_ptr+i))) {
      return 1;
    } else {
      continue;
    }
  }

  if (s1.length() == s2.length()) {
    return 0;
  }
  if (s1.length() > s2.length()) {
    return 1;
  } else {
    return -1;
  }
}

static bool IsInColumnList(const string &column) {
  if (g_column_list.empty()) return true;
  for (vector<string>::const_iterator it = g_column_list.begin(); it != g_column_list.end(); ++it) {
    if (0 == CompareNoCase(*it, column)) {
      return true;
    }
  }
  return false;
}

static bool IsInEventTypeList(const int32_t &event_type) {
  if (g_event_type_list.empty()) return true;
  for (vector<int>::const_iterator it = g_event_type_list.begin(); it != g_event_type_list.end(); ++it) {
    if ((*it) == event_type) {
      return true;
    }
  }
  return false;
}


static void metadata_print(const std::string &topic,
                           const RdKafka::Metadata *metadata) {
  std::cout << "Metadata for " << (topic.empty() ? "" : "all topics")
            << "(from broker " << metadata->orig_broker_id() << ":"
            << metadata->orig_broker_name() << std::endl;

  /* Iterate brokers */
  std::cout << " " << metadata->brokers()->size() << " brokers:" << std::endl;
  RdKafka::Metadata::BrokerMetadataIterator ib;
  for (ib = metadata->brokers()->begin(); ib != metadata->brokers()->end();
       ++ib) {
    std::cout << "  broker " << (*ib)->id() << " at " << (*ib)->host() << ":"
              << (*ib)->port() << std::endl;
  }

  /* Iterate topics */
  std::cout << metadata->topics()->size() << " topics:" << std::endl;
  RdKafka::Metadata::TopicMetadataIterator it;
  for (it = metadata->topics()->begin(); it != metadata->topics()->end();
       ++it) {
    std::cout << "  topic \"" << (*it)->topic() << "\" with "
              << (*it)->partitions()->size() << " partitions:";

    if ((*it)->err() != RdKafka::ERR_NO_ERROR) {
      std::cout << " " << err2str((*it)->err());
      if ((*it)->err() == RdKafka::ERR_LEADER_NOT_AVAILABLE)
        std::cout << " (try again)";
    }
    std::cout << std::endl;

    /* Iterate topic's partitions */
    RdKafka::TopicMetadata::PartitionMetadataIterator ip;
    for (ip = (*it)->partitions()->begin(); ip != (*it)->partitions()->end();
         ++ip) {
      std::cout << "    partition " << (*ip)->id() << ", leader "
                << (*ip)->leader() << ", replicas: ";

      /* Iterate partition's replicas */
      RdKafka::PartitionMetadata::ReplicasIterator ir;
      for (ir = (*ip)->replicas()->begin(); ir != (*ip)->replicas()->end();
           ++ir) {
        std::cout << (ir == (*ip)->replicas()->begin() ? "" : ",") << *ir;
      }

      /* Iterate partition's ISRs */
      std::cout << ", isrs: ";
      RdKafka::PartitionMetadata::ISRSIterator iis;
      for (iis = (*ip)->isrs()->begin(); iis != (*ip)->isrs()->end(); ++iis)
        std::cout << (iis == (*ip)->isrs()->begin() ? "" : ",") << *iis;

      if ((*ip)->err() != RdKafka::ERR_NO_ERROR)
        std::cout << ", " << RdKafka::err2str((*ip)->err()) << std::endl;
      else
        std::cout << std::endl;
    }
  }
}

static bool run = true;
static bool exit_eof = false;

static void sigterm(int sig) { (void) sig; run = false; }

class ExampleDeliveryReportCb : public RdKafka::DeliveryReportCb {
 public:
  void dr_cb(RdKafka::Message &message) {
    std::cout << "Message delivery for (" << message.len()
              << " bytes): " << message.errstr() << std::endl;
    if (message.key())
      std::cout << "Key: " << *(message.key()) << ";" << std::endl;
  }
};

class ExampleEventCb : public RdKafka::EventCb {
 public:
  void event_cb(RdKafka::Event &event) {
    switch (event.type()) {
      case RdKafka::Event::EVENT_ERROR:
        std::cerr << "ERROR (" << RdKafka::err2str(event.err())
                  << "): " << event.str() << std::endl;
        if (event.err() == RdKafka::ERR__ALL_BROKERS_DOWN) run = false;
        break;

      case RdKafka::Event::EVENT_STATS:
        std::cerr << "\"STATS\": " << event.str() << std::endl;
        break;

      case RdKafka::Event::EVENT_LOG:
        fprintf(stderr, "LOG-%i-%s: %s\n", event.severity(),
                event.fac().c_str(), event.str().c_str());
        break;

      default:
        std::cerr << "EVENT " << event.type() << " ("
                  << RdKafka::err2str(event.err()) << "): " << event.str()
                  << std::endl;
        break;
    }
  }
};

/* Use of this partitioner is pretty pointless since no key is provided
 * in the produce() call. */
class MyHashPartitionerCb : public RdKafka::PartitionerCb {
 public:
  int32_t partitioner_cb(const RdKafka::Topic *topic, const std::string *key,
                         int32_t partition_cnt, void *msg_opaque) {
    (void)topic, (void)key, (void)msg_opaque;                         
    return djb_hash(key->c_str(), key->size()) % partition_cnt;
  }

 private:
  static inline unsigned int djb_hash(const char *str, size_t len) {
    unsigned int hash = 5381;
    for (size_t i = 0; i < len; i++) hash = ((hash << 5) + hash) + str[i];
    return hash;
  }
};

static inline bool HasWantDbAndTable(const ::rdp::messages::Event &event) {
  if (g_schema_name.empty()) return true;
  if (event.has_database_name() && event.database_name() == g_schema_name) {
    if (g_table_name.empty()) return true;
    if (event.has_table_name() && event.table_name() == g_table_name) return true;
  }
  return false;
}

bool NeedOutputTransaction(const ::rdp::messages::Transaction *trans) {
  assert(trans != NULL);

  for (int i = 0; i < trans->events_size(); i++) {
    const ::rdp::messages::Event &event = trans->events(i);

    if (!IsInsUpDelEvent(event.event_type())) continue;

    if (!HasWantDbAndTable(event)) continue;

    if (IsInEventTypeList(event.event_type())) return true;
  }
  return false;
}

void PrintColumnName(const ::rdp::messages::Transaction *trans) {
  assert(trans != NULL);

  bool had_print_column_name = false;
  std::cout << "offset|rdp-timestamp|timestamp|database|table|operation";

  for (int i = 0; i <trans->events_size() ; i++) {
    const ::rdp::messages::Event &event = trans->events(i);
    if (!IsInsUpDelEvent(event.event_type())) continue;

    for (int j = 0; j < event.rows_size(); j++) {
      const ::rdp::messages::Row &row = event.rows(j);
      if (row.before_size() > 0) {
        for (int k = 0; k < row.before_size(); k++) {
          if (IsInColumnList(row.before(k).name())) std::cout << "|" << row.before(k).name();
        }
        had_print_column_name = true;
        break;
      }
      if (row.after_size() > 0) {
        for (int k = 0; k < row.after_size(); k++) {
          if (IsInColumnList(row.after(k).name())) std::cout << "|" << row.after(k).name();
        }
        had_print_column_name = true;
        break;
      }
    }
    if (had_print_column_name) break;
  }

  std::cout << std::endl;
}

void PrintColumnValue(const ::rdp::messages::Transaction *trans) {
  assert(trans != NULL);

  for (int i = 0; i <trans->events_size() ; i++) {
    const ::rdp::messages::Event &event = trans->events(i);
    if (!IsInsUpDelEvent(event.event_type()) || !HasWantDbAndTable(event) || !IsInEventTypeList(event.event_type())) continue;

    for (int j = 0; j < event.rows_size(); j++) {
      const ::rdp::messages::Row &row = event.rows(j);
      if (row.before_size() > 0) {
        std::cout << g_offset << "|" << event.timestamp_of_receipt() << "|" << event.timestamp() << "|" << event.database_name() << "|" << event.table_name();
        if (event.event_type() == UPDATE_ROWS_EVENT || event.event_type() == UPDATE_ROWS_EVENT_V1)
          std::cout << "|befor:" << get_type_str(event.event_type());
        else
          std::cout << "|" << get_type_str(event.event_type());
        for (int k = 0; k < row.before_size(); k++) {
          if (IsInColumnList(row.before(k).name())) std::cout << "|" << row.before(k).value();
        }
        std::cout << std::endl;
      }
      if (row.after_size() > 0) {
        std::cout << g_offset << "|" << event.timestamp_of_receipt() << "|" << event.timestamp() << "|" << event.database_name() << "|" << event.table_name();
        if (event.event_type() == UPDATE_ROWS_EVENT || event.event_type() == UPDATE_ROWS_EVENT_V1)
          std::cout << "|after:" << get_type_str(event.event_type());
        else
          std::cout << "|" << get_type_str(event.event_type());
        for (int k = 0; k < row.after_size(); k++) {
          if (IsInColumnList(row.after(k).name())) std::cout << "|" << row.after(k).value();
        }
        std::cout << std::endl;
      }
    }
  }
}

void PrintTransConcise(const ::rdp::messages::Transaction *trans) {

  if (!NeedOutputTransaction(trans)) return;

  std::cout << "gtid:" << trans->gtid() << std::endl;

  PrintColumnName(trans);
  PrintColumnValue(trans);

  std::cout << std::endl;
}

void PrintTrans(::rdp::messages::Transaction *trans) {
  std::cout << "--------------------Transaction-----------------" << std::endl;
  std::cout << "Gtid:" << trans->gtid() << ",Seqno:" << trans->seq()
            << ",Position:" << trans->position()
            << ",Binlog File Name:" << trans->binlog_file_name()
            << ",Next position:" << trans->next_position()
            << ",Next binlog file name:" << trans->next_binlog_file_name()
            << ",Events number:" << trans->events_size() << std::endl;

  int isz = trans->events_size();
  for (int i = 0; i < isz; i++) {
    //std::cout << "-----------Event Index: " << i << "------------" << std::endl;
    ::rdp::messages::Event *event = trans->mutable_events(i);

    std::cout << "Event Type" << "[" << event->event_type() << "]:" << get_type_str(event->event_type());

    if (!IsInEventTypeList(event->event_type())) continue;

    if (!HasWantDbAndTable(*event)) continue;

    std::cout << ",Database Name:" << event->database_name();

    std::cout << ",Table Name:" << event->table_name();

    if (event->has_schema_id()) {
      std::cout << ",Schema Id:" << event->schema_id();
    }

    std::cout << ",Timestamp:" << event->timestamp()
              << ",Timestamp Of receipt:" << event->timestamp_of_receipt()
              << ",Position:" << event->position()
              << ",Binlog file name:" << event->binlog_file_name()
              << ",Next position:" << event->next_position();

    int isz2 = event->rows_size();

    if ((event->event_type() >= WRITE_ROWS_EVENT && event->event_type() <= DELETE_ROWS_EVENT ) || 
        (event->event_type() >= WRITE_ROWS_EVENT_V1 && event->event_type() <= DELETE_ROWS_EVENT_V1)) {
      std::cout <<  ",Row count:" << isz2;
    }

    if (event->event_type() == QUERY_EVENT) {
      std::cout <<  ",Sql:" << event->sql_statement();
    }

    std::cout << std::endl;

    for (int j = 0; j < isz2; j++) {
      ::rdp::messages::Row *row = event->mutable_rows(j);

      int isz_before = row->before_size();

      if (isz_before) {
        std::cout << "[" << j << "]" << "Before row-->" << "Column count:" << isz_before << "-->";
      }

      for (int x = 0; x < isz_before; x++) {
        ::rdp::messages::Column *column = row->mutable_before(x);
        if (!IsInColumnList(column->name())) continue;
        std::cout << x << ":" << column->name() << ":" << column->type() << ":" << column->value();
        if (x < isz_before - 1) {
          std::cout << "|";
        }
      }
      std::cout << std::endl;

      int isz_after = row->after_size();

      std::cout << "[" << j << "]" << "After  row-->" << "Column count:" << isz_after << "-->";
      for (int x = 0; x < isz_after; x++) {
        ::rdp::messages::Column *column = row->mutable_after(x);
        if (!IsInColumnList(column->name())) continue;
        std::cout << x << ":" << column->name() << ":" << column->type() << ":" << column->value();
        if (x < isz_after - 1) {
          std::cout << "|";
        }
      }
      std::cout << std::endl;
    }
    //std::cout << "--------------Event End--------------" << std::endl;
  }
  std::cout <<"--------------------Transaction End-------------" << std::endl;
}

void PrintKafkaPkg(const ::rdp::messages::KafkaPkg *pkg) {
  std::cout << "********************KafkaPkg********************" << std::endl;
  std::cout << "Gtid:" << pkg->gtid() << ",Seqno:" << pkg->seq_no()
            << ",Split flag:" << pkg->split_flag()
            << ",Epoch:" << pkg->epoch()
            << ",Transaction Seq No:" << pkg->trans_seq_no()
            << ",Data length:" << pkg->data().length()
            << ",Source data len:" << pkg->source_data_len()
            << ",Checksum:" << pkg->checksum() 
            << ", version:[" << pkg->has_version() << "] " << pkg->version() << std::endl;
}

static rdp::messages::Transaction *KafkaPkgToTransaction(rdp::messages::KafkaPkg *pkg, rdp::messages::Transaction *trans) {
  assert(pkg != NULL);
  rdp::messages::Transaction *trans_pkg = NULL;

  if (trans == NULL)
    trans = new rdp::messages::Transaction();

  if (pkg->has_flag() && (pkg->flag() & rdp::messages::kKfkPkgCompressData) == rdp::messages::kKfkPkgCompressData) {
    if (pkg->has_source_data_len() && g_decompress_buf_size < pkg->source_data_len()) {
      g_decompress_buf_size = pkg->source_data_len() > g_decompress_buf_size*1.5 ? pkg->source_data_len() : g_decompress_buf_size*1.5; 
      g_decompress_buf = (char *)realloc(g_decompress_buf, g_decompress_buf_size);
      assert(g_decompress_buf != NULL);
    }
    size_t data_size = Decompress(pkg->data().c_str(), pkg->data().length(), &g_decompress_buf, &g_decompress_buf_size);
    if (data_size <= 0) {
      std::cerr << "Decompress Error" << std::endl;
      return NULL;
    }
    if(!concise_print) std::cout << "Transaction size:" << pkg->data().length() << ", after decompressed:" << data_size;
    if (!trans->IsInitialized()) {
      if (!trans->ParseFromArray(g_decompress_buf, data_size)) {
        std::cerr << "ParseFromString failed" << std::endl;
        return NULL;
      }
      if (!concise_print) PrintTrans(trans);
      return trans;
    } else {
      trans_pkg = new rdp::messages::Transaction();
      if (!trans_pkg->ParseFromArray(g_decompress_buf, data_size)) {
        std::cerr << "ParseFromString failed" << std::endl;
        delete trans_pkg;
        return NULL;
      }
    }
  } else {
    if (!trans->IsInitialized()) {
      if (!trans->ParseFromString(pkg->data())) {
        std::cerr << "ParseFromString failed" << std::endl;
        return NULL;
      }
      if (!concise_print) PrintTrans(trans);
      return trans;
    } else {
      trans_pkg = new rdp::messages::Transaction();
      if (!trans_pkg->ParseFromString(pkg->data())) {
        std::cerr << "ParseFromString failed" << std::endl;
        delete trans_pkg;
        return NULL;
      }
    }
  }

  if (!concise_print) PrintTrans(trans_pkg);

  assert(trans_pkg != NULL);
  assert(trans->IsInitialized() == true);
  assert(trans->gtid() == trans_pkg->gtid());
  assert(trans->seq() == trans_pkg->seq());

  trans->mutable_events()->MergeFrom(trans_pkg->events());

  delete trans_pkg;
   
  return trans;
}


void handle_binlog_event(RdKafka::Message *message) {
  ::rdp::messages::VMSMessage *vms_msg = new ::rdp::messages::VMSMessage;
  assert(NULL != vms_msg);

#if 0
  printf("%lu\n", message->len());
  for (unsigned long i = 0; i < message->len(); ++i) {
    printf("%02x", (unsigned char)(((char*)message->payload())[i]));
  }
  printf("\n");
#endif


  if (!vms_msg->ParseFromArray(message->payload(), message->len())) {
    std::cerr << "ParseFromArray VMSMessage Failt" << std::endl;
    exit(1);
  }

  ::rdp::messages::KafkaPkg *pkg = new rdp::messages::KafkaPkg;
  assert(NULL != pkg);

  if (!pkg->ParseFromString(vms_msg->payload())) {
    std::cerr << "ParseFromArray KafkaPkg Failt" << std::endl;
    exit(1);
  }

  if (!concise_print) std::cout << "msgid:" << vms_msg->messageid() << std::endl;

  delete vms_msg;
  vms_msg = NULL;

  if (!concise_print) PrintKafkaPkg(pkg);

  rdp::messages::Transaction *trans = KafkaPkgToTransaction(pkg, NULL);
  assert(NULL != trans);

  //std::cout << pb2json(*trans) << std::endl;
  
  if (concise_print)
    PrintTransConcise(trans);

  delete pkg;
  delete trans;

  return;
}

void msg_consume(RdKafka::Message *message, void *opaque) {
  (void )opaque;
  switch (message->err()) {
    case RdKafka::ERR__TIMED_OUT:
      break;

    case RdKafka::ERR_NO_ERROR:
      /* Real message */
      //std::cout << "---------------------------------------------" << std::endl;
      g_offset = message->offset();
      if (!concise_print) {
        std::cout << "Read msg at offset:" << message->offset()
          << ",Read index:" << message_read_index++ << std::endl;
        if (message->key()) {
          std::cout << "Key:" << *message->key() << std::endl;
        }
      }
      handle_binlog_event(message);
      //std::cout << "---------------------------------------------" << std::endl;
      ++read_num;
      if (continue_read != "B" && read_len > 0 && read_num >= read_len) {
        if (!concise_print) std::cout << "Read end, Read message:" << read_num << ",Reach threshold:" << read_len << std::endl;
        run = false;
      }
      if (end_offset != RdKafka::Topic::OFFSET_END &&
          message->offset() >= end_offset) {
        if (!concise_print) std::cout << "Read end, Read offset:" << message->offset() << ",Reach end offset:" << end_offset << std::endl;
        run = false;
      }
      break;

    case RdKafka::ERR__PARTITION_EOF:
      /* Last message */
      if (exit_eof) {
        run = false;
      }
      break;

    case RdKafka::ERR__UNKNOWN_TOPIC:
    case RdKafka::ERR__UNKNOWN_PARTITION:
      std::cerr << "Consume failed: " << message->errstr() << std::endl;
      run = false;
      break;

    default:
      /* Errors */
      std::cerr << "Consume failed: " << message->errstr() << std::endl;
      run = false;
  }
}

class ExampleConsumeCb : public RdKafka::ConsumeCb {
 public:
  void consume_cb(RdKafka::Message &msg, void *opaque) {
    msg_consume(&msg, opaque);
  }
};

int main(int argc, char **argv) {
  std::string brokers = "localhost";
  std::string errstr;
  std::string topic_str;
  std::string mode;
  std::string debug;
  std::string b_version_fback;
  int32_t partition = RdKafka::Topic::PARTITION_UA;
  int64_t start_offset = RdKafka::Topic::OFFSET_BEGINNING;
  bool do_conf_dump = false;
  int opt;
  MyHashPartitionerCb hash_partitioner;
  int use_ccb = 0;

  /*
   * Create configuration objects
   */
  RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
  RdKafka::Conf *tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);

  while ((opt = getopt(argc, argv, "PCLt:p:b:z:qd:o:eX:AM:f:E:l:y:Z:BwS:T:J:c:D")) != -1) {
    switch (opt) {
      case 'P':
      case 'C':
      case 'L':
        mode = opt;
        break;

      case 't':
        topic_str = optarg;
        break;
      case 'S':
        g_schema_name = optarg;
        break;

      case 'T':
        g_table_name = optarg;
        break;

      case 'J':
        if (!ToEventTypeList(optarg)) {
          std::cerr << " -J <event_type_list> \"" << optarg << "\" error, not int [23,24,25,30,31,32]" << std::endl;
          exit(-1);
        }
        break;

      case 'p':
        if (!strcmp(optarg, "random"))
          /* default */;
        else if (!strcmp(optarg, "hash")) {
          if (tconf->set("partitioner_cb", &hash_partitioner, errstr) !=
              RdKafka::Conf::CONF_OK) {
            std::cerr << errstr << std::endl;
            exit(1);
          }
        } else
          partition = std::atoi(optarg);
        break;

      case 'b':
        brokers = optarg;
        break;

      case 'z':
        if (conf->set("compression.codec", optarg, errstr) !=
            RdKafka::Conf::CONF_OK) {
          std::cerr << errstr << std::endl;
          exit(1);
        }
        break;

      case 'o':
        if (!strcmp(optarg, "end"))
          start_offset = RdKafka::Topic::OFFSET_END;
        else if (!strcmp(optarg, "beginning"))
          start_offset = RdKafka::Topic::OFFSET_BEGINNING;
        else if (!strcmp(optarg, "stored"))
          start_offset = RdKafka::Topic::OFFSET_STORED;
        else
          start_offset = strtoll(optarg, NULL, 10);
        break;

      case 'e':
        exit_eof = true;
        break;

      case 'd':
        debug = optarg;
        break;

      case 'M':
        if (conf->set("statistics.interval.ms", optarg, errstr) !=
            RdKafka::Conf::CONF_OK) {
          std::cerr << errstr << std::endl;
          exit(1);
        }
        break;

      case 'X': {
        char *name, *val;

        if (!strcmp(optarg, "dump")) {
          do_conf_dump = true;
          continue;
        }

        name = optarg;
        if (!(val = strchr(name, '='))) {
          std::cerr << "%% Expected -X property=value, not " << name
                    << std::endl;
          exit(1);
        }

        *val = '\0';
        val++;

        /* Try "topic." prefixed properties on topic
         * conf first, and then fall through to global if
         * it didnt match a topic configuration property. */
        RdKafka::Conf::ConfResult res;
        if (!strncmp(name, "topic.", strlen("topic.")))
          res = tconf->set(name + strlen("topic."), val, errstr);
        else
          res = conf->set(name, val, errstr);

        if (res != RdKafka::Conf::CONF_OK) {
          std::cerr << errstr << std::endl;
          exit(1);
        }
      } break;

      case 'f':
        if (!strcmp(optarg, "ccb"))
          use_ccb = 1;
        else {
          std::cerr << "Unknown option: " << optarg << std::endl;
          exit(1);
        }
        break;

      case 'E':
        if (!strcmp(optarg, "end"))
          end_offset = RdKafka::Topic::OFFSET_END;
        else
          end_offset = strtoll(optarg, NULL, 10);
        break;

      case 'l':
        read_len = strtoll(optarg, NULL, 10);
        break;

      case 'B':
        continue_read = opt;
        break;

      case 'Z':
        g_decompress_buf_size = strtol(optarg, NULL, 10);
        break;


      case 'w':
        get_water = true;
        break;

      case 'y':
        b_version_fback = optarg;

        if (RdKafka::Conf::CONF_OK !=
            conf->set("broker.version.fallback", b_version_fback, errstr)) {
          std::cerr
              << "Kafka Consumer Set Configure 'broker.version.fallback': "
              << b_version_fback << ", Failt: " << errstr << std::endl;
          ;
          exit(1);
        }
        break;

      case 'c':
        if (!ToColumnList(optarg)) {
          std::cerr << " -c <column list> \"" << optarg << "\" error" << std::endl;
          exit(-1);
        }
        break;

      case 'D':
        concise_print = true;
        break;

      default:
        goto usage;
    }
  }

  if (mode.empty() || (topic_str.empty() && mode != "L") || optind != argc) {
  usage:
    std::string features;
    conf->get("builtin.features", features);
    fprintf(stderr,
            "Usage: %s [-C|-P] -t <topic> "
            "[-p <partition>] [-b <host1:port1,host2:port2,..>]\n"
            "\n"
            "librdkafka version %s (0x%08x, builtin.features \"%s\")\n"
            "\n"
            " Options:\n"
            "  -C | -P         Consumer or Producer mode\n"
            "  -L              Metadata list mode\n"
            "  -S <schema>     database name\n"
            "  -T <table>      table name\n"
            "  -t <topic>      Topic to fetch / produce\n"
            "  -p <num>        Partition (random partitioner)\n"
            "  -p <func>       Use partitioner:\n"
            "                  random (default), hash\n"
            "  -b <brokers>    Broker address (localhost:9092)\n"
            "  -z <codec>      Enable compression:\n"
            "                  none|gzip|snappy\n"
            "  -o <offset>     Start offset (consumer)\n"
            "  -e              Exit consumer when last message\n"
            "                  in partition has been received.\n"
            "  -d [facs..]     Enable debugging contexts:\n"
            "                  %s\n"
            "  -M <intervalms> Enable statistics\n"
            "  -X <prop=name>  Set arbitrary librdkafka "
            "configuration property\n"
            "                  Properties prefixed with \"topic.\" "
            "will be set on topic object.\n"
            "                  Use '-X list' to see the full list\n"
            "                  of supported properties.\n"
            "  -f <flag>       Set option:\n"
            "                     ccb - use consume_callback\n"
            "  -E <offset>     End offset (consumer)\n"
            "  -l <len>        Read message number (consumer)\n"
            "  -B              Batch read (consumer)\n"
            "  -y <broker.version.fallback>  eg: 0.8.1\n"
            "  -Z <decompres buffer size>\n"
            "  -J <event type> mysql insert:30 update:31 delete:32\n"
            "                  mariadb insert:23 update:24 delete:25\n"
            "                  eg: 30,31,32\n"
            "  -c <column list> column name list, ignore case\n"
            "                   eg: column_1,column_2\n"
            "  -D              concise print transaction info\n"
            "  -w              print kafka topic low and high offset\n"
            "\n"
            " In Consumer mode:\n"
            "  writes fetched messages to stdout\n"
            " In Producer mode:\n"
            "  reads messages from stdin and sends to broker\n"
            "\n"
            "\n"
            "\n",
            argv[0], RdKafka::version_str().c_str(), RdKafka::version(),
            features.c_str(), RdKafka::get_debug_contexts().c_str());
    exit(1);
  }

  /*
   * Set configuration properties
   */
  conf->set("metadata.broker.list", brokers, errstr);

  if (!debug.empty()) {
    if (conf->set("debug", debug, errstr) != RdKafka::Conf::CONF_OK) {
      std::cerr << errstr << std::endl;
      exit(1);
    }
  }

  ExampleEventCb ex_event_cb;
  conf->set("event_cb", &ex_event_cb, errstr);

  if (do_conf_dump) {
    int pass;

    for (pass = 0; pass < 2; pass++) {
      std::list<std::string> *dump;
      if (pass == 0) {
        dump = conf->dump();
        std::cout << "# Global config" << std::endl;
      } else {
        dump = tconf->dump();
        std::cout << "# Topic config" << std::endl;
      }

      for (std::list<std::string>::iterator it = dump->begin();
           it != dump->end();) {
        std::cout << *it << " = ";
        it++;
        std::cout << *it << std::endl;
        it++;
      }
      std::cout << std::endl;
    }
    exit(0);
  }

  signal(SIGINT, sigterm);
  signal(SIGTERM, sigterm);

  if (mode == "P") {
    /*
     * Producer mode
     */

    if (topic_str.empty()) goto usage;

    ExampleDeliveryReportCb ex_dr_cb;

    /* Set delivery report callback */
    conf->set("dr_cb", &ex_dr_cb, errstr);

    /*
     * Create producer using accumulated global configuration.
     */
    RdKafka::Producer *producer = RdKafka::Producer::create(conf, errstr);
    if (!producer) {
      std::cerr << "Failed to create producer: " << errstr << std::endl;
      exit(1);
    }

    std::cout << "% Created producer " << producer->name() << std::endl;

    /*
     * Create topic handle.
     */
    RdKafka::Topic *topic =
        RdKafka::Topic::create(producer, topic_str, tconf, errstr);
    if (!topic) {
      std::cerr << "Failed to create topic: " << errstr << std::endl;
      exit(1);
    }

    /*
     * Read messages from stdin and produce to broker.
     */
    for (std::string line; run && std::getline(std::cin, line);) {
      if (line.empty()) {
        producer->poll(0);
        continue;
      }

      /*
       * Produce message
       */
      RdKafka::ErrorCode resp = producer->produce(
          topic, partition, RdKafka::Producer::RK_MSG_COPY /* Copy payload */,
          const_cast<char *>(line.c_str()), line.size(), NULL, NULL);
      if (resp != RdKafka::ERR_NO_ERROR)
        std::cerr << "% Produce failed: " << RdKafka::err2str(resp)
                  << std::endl;
      else
        std::cerr << "% Produced message (" << line.size() << " bytes)"
                  << std::endl;

      producer->poll(0);
    }
    run = true;

    while (run && producer->outq_len() > 0) {
      std::cerr << "Waiting for " << producer->outq_len() << std::endl;
      producer->poll(1000);
    }

    delete topic;
    delete producer;

  } else if (mode == "C") {
    /*
     * Consumer mode
     */

    if (topic_str.empty()) goto usage;


    /*
     * Create consumer using accumulated global configuration.
     */
    RdKafka::Consumer *consumer = RdKafka::Consumer::create(conf, errstr);
    if (!consumer) {
      std::cerr << "Failed to create consumer: " << errstr << std::endl;
      exit(1);
    }

    if (!concise_print) std::cout << "% Created consumer " << consumer->name() << std::endl;

    /*
     * Create topic handle.
     */
    RdKafka::Topic *topic =
        RdKafka::Topic::create(consumer, topic_str, tconf, errstr);
    if (!topic) {
      std::cerr << "Failed to create topic: " << errstr << std::endl;
      exit(1);
    }

    /*
     * Start consumer for topic+partition at start offset
     */
    if (!concise_print) std::cout << "consumer->start: " << topic << "\t" << partition << "\t" << start_offset << std::endl;
    RdKafka::ErrorCode resp = consumer->start(topic, partition, start_offset);
    if (resp != RdKafka::ERR_NO_ERROR) {
      std::cerr << "Failed to start consumer: " << RdKafka::err2str(resp)
                << std::endl;
      exit(1);
    }

    ExampleConsumeCb ex_consume_cb;

    /*
     * Consume messages
     */
    while (run) {
      if (use_ccb) {
        consumer->consume_callback(topic, partition, 1000, &ex_consume_cb,
                                   &use_ccb);
      } else {
        RdKafka::Message *msg = consumer->consume(topic, partition, 1000);
        msg_consume(msg, NULL);
        delete msg;
      }

      if (continue_read == "B" && read_num >= read_len) {
        std::cout << "Press Any Key To Continue Or 'exit' To Quit: ";
        read_num = 0;
        std::string user_in;
        std::cin >> user_in;
        if (0 == user_in.compare("exit")) {
          run = false;
        }
      }

      consumer->poll(0);
    }

    if (get_water) {
      int64_t low = 0;
      int64_t high = 0;

      RdKafka::ErrorCode ret = consumer->query_watermark_offsets(topic_str, partition, &low, &high, 5000);
      if (ret) {
        std::cerr << RdKafka::err2str(ret) << ", ret"<< ret;
        exit(1);
      }

      std::cout << "low:"<<low << "," << "high:" << high << std::endl;
    }

    /*
     * Stop consumer
     */
    consumer->stop(topic, partition);

    consumer->poll(1000);

    delete topic;
    delete consumer;

  } else {
    /* Metadata mode */

    /*
     * Create producer using accumulated global configuration.
     */
    RdKafka::Producer *producer = RdKafka::Producer::create(conf, errstr);
    if (!producer) {
      std::cerr << "Failed to create producer: " << errstr << std::endl;
      exit(1);
    }

    std::cout << "% Created producer " << producer->name() << std::endl;

    /*
     * Create topic handle.
     */
    RdKafka::Topic *topic = NULL;
    if (!topic_str.empty()) {
      topic = RdKafka::Topic::create(producer, topic_str, tconf, errstr);
      if (!topic) {
        std::cerr << "Failed to create topic: " << errstr << std::endl;
        exit(1);
      }
    }

    while (run) {
      class RdKafka::Metadata *metadata;

      /* Fetch metadata */
      RdKafka::ErrorCode err =
          producer->metadata(topic != NULL, topic, &metadata, 5000);
      if (err != RdKafka::ERR_NO_ERROR) {
        std::cerr << "%% Failed to acquire metadata: " << RdKafka::err2str(err)
                  << std::endl;
        run = 0;
        break;
      }

      metadata_print(topic_str, metadata);

      delete metadata;
      run = 0;
    }
  }

  /*
   * Wait for RdKafka to decommission.
   * This is not strictly needed (when check outq_len() above), but
   * allows RdKafka to clean up all its resources before the application
   * exits so that memory profilers such as valgrind wont complain about
   * memory leaks.
   */
  RdKafka::wait_destroyed(5000);
  FREE_BUF(g_decompress_buf);

  return 0;
}
