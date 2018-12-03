#
#Copyright 2018 vip.com.
#
#Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
#the License. You may obtain a copy of the License at
#
#http://www.apache.org/licenses/LICENSE-2.0
#
#Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
#an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
#specific language governing permissions and limitations under the License.
#

set(DEPS_DIR ${CMAKE_SOURCE_DIR}/client/deps)

INCLUDE_DIRECTORIES(
  ${CMAKE_SOURCE_DIR}/client
  ${CMAKE_SOURCE_DIR}/include
  ${CMAKE_SOURCE_DIR}/client/message/
  ${CMAKE_SOURCE_DIR}/mysys_ssl
  ${ZLIB_INCLUDE_DIR}
  ${LZ4_INCLUDE_DIR}
  ${SSL_INCLUDE_DIRS}
  ${CMAKE_SOURCE_DIR}/libmysql
  ${CMAKE_SOURCE_DIR}/libbinlogevents/include
  ${CMAKE_SOURCE_DIR}/regex
  ${CMAKE_SOURCE_DIR}/strings
  ${DEPS_DIR}/protobuf/include
  ${DEPS_DIR}/boost/include
  ${DEPS_DIR}/glog/include
  ${DEPS_DIR}/jansson/include
  ${DEPS_DIR}/rdp-comm/include
  ${DEPS_DIR}/zklib/include
  ${DEPS_DIR}/gflags/include
  ${DEPS_DIR}/librdkafka/include
  ${DEPS_DIR}/gtest/include
  ${DEPS_DIR}/tbb/include
  ${DEPS_DIR}/lz4/include
  ${EDITLINE_INCLUDE_DIR} 
  ${CMAKE_CURRENT_BINARY_DIR}
)

INCLUDE(${MYSQL_CMAKE_SCRIPT_DIR}/compile_flags.cmake)

## Subdirectory with common client code.
ADD_SUBDIRECTORY(base)

## We will need libeay32.dll and ssleay32.dll when running client executables.
#COPY_OPENSSL_DLLS(copy_openssl_client)

INCLUDE(${MYSQL_CMAKE_SCRIPT_DIR}/compile_flags.cmake)

ADD_DEFINITIONS(${SSL_DEFINES})
ADD_DEFINITIONS(-DRDP_GIT_HASH="${RDP_GIT_HASH}")

## Subdirectory for mysqlpump
ADD_SUBDIRECTORY(dump)


if (NO_KAFKA MATCHES 1)
  MESSAGE(WARNING "Will NOT write to kafka")
  ADD_DEFINITIONS(-DNO_KAFKA)
endif()

if (FIU MATCHES 1)
  MESSAGE(WARNING "Using FIU Test")
  ADD_DEFINITIONS(-DFIU)
endif()

if (STAT_TIME MATCHES 1)
  MESSAGE(WARNING "Write RTT Stat Data")
  ADD_DEFINITIONS(-DSTAT_TIME)
endif()

SET(SRC_LIST
  syncer_progress.cc
  slave_view.cc
  syncer_slave.cc
  syncer_app.cc
  syncer_utils.cc
  syncer_conf.cc
  binlog_writer.cc
  binlog_reader.cc
  binlog_parser.cc
  trx_boundary_parser.cc
  bounded_buffer.cc
  mysql_gtidmgr.cc
  log_event_parser.cc
  rows_log_event_ex.cc
  parsing_worker.cc
  parsing_coordinator.cc
  mem_block.cc
  row_record.cc
  syncer_events.cc
  encoded_msg_map.cc
  syncer_main.h
  syncer_progress.h
  syncer_slave.h
  syncer_app.h
  syncer_utils.h
  syncer_conf.h
  binlog_writer.h
  binlog_reader.h
  binlog_parser.h
  syncer_consts.h
  syncer_incl.h
  trx_boundary_parser.h
  bounded_buffer.h
  mysql_gtidmgr.h
  syncer_def.h
  log_event_parser.h
  rows_log_event_ex.h
  parsing_worker.h
  parsing_coordinator.h
  mem_block.h
  rate_limiter.cc
  row_record.h
  syncer_events.h
  encoded_msg_map.h
  schema_manager.cc
  util_sub_process.cc
  kafka_producer.h
  kafka_producer.cc
  jansson_pack.h
  jansson_pack.cc
  checkpoint.h
  checkpoint.cc
  checksum.cc
  checksum.h
  statistics_writer.cc
  statistics_writer.h
  split_package.cc
  split_package.h
  parsing_trx.cc
  parsing_trx.h
  memory_pool.h
  memory_pool.cc
  rdp_syncer_metric.h
  rdp_syncer_metric.cc
  metric_instanc.h
  metric_instanc.cc
  syncer_filter.h
  syncer_filter.cc
  rebuild.h
  rebuild.cc
 )

MYSQL_ADD_EXECUTABLE(rdp_syncer 
  syncer_main.cc
  ${SRC_LIST}
)

MYSQL_ADD_EXECUTABLE(mysqldump mysqldump.c ../sql-common/my_user.c)                                                                                                                      
TARGET_LINK_LIBRARIES(mysqldump mysqlclient)
SET_TARGET_PROPERTIES (mysqldump PROPERTIES HAS_CXX TRUE)

ADD_COMPILE_FLAGS(
syncer_main.cc 
  COMPILE_FLAGS "-I${CMAKE_SOURCE_DIR}/sql" "-DHAVE_REPLICATION" "-DDISABLE_PSI_MUTEX"
)
TARGET_LINK_LIBRARIES(rdp_syncer mysqlclient binlogevents_static curl
  ${CMAKE_SOURCE_DIR}/client/message/librdp_message.so
  ${DEPS_DIR}/protobuf/lib/libprotobuf.so
  ${DEPS_DIR}/glog/lib/libglog.so
  ${DEPS_DIR}/boost/lib/libboost_system.so
  ${DEPS_DIR}/boost/lib/libboost_thread.so
  ${DEPS_DIR}/boost/lib/libboost_regex.so
  ${DEPS_DIR}/jansson/lib/libjansson.so
  ${DEPS_DIR}/zklib/lib/libzookeeper_mt.so
  ${DEPS_DIR}/rdp-comm/lib/librdp-comm.so
  ${DEPS_DIR}/gflags/lib/libgflags.so
  ${DEPS_DIR}/librdkafka/lib/librdkafka++.so
  ${DEPS_DIR}/tbb/lib/intel64/gcc4.7/libtbb.so
  ${DEPS_DIR}/tbb/lib/intel64/gcc4.7/libtbbmalloc.so
  ${DEPS_DIR}/tbb/lib/intel64/gcc4.7/libtbb_preview.so
  ${DEPS_DIR}/lz4/lib/liblz4.so
)

ADD_DEFINITIONS(-DHAVE_DLOPEN)

