CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
PROJECT(rdp_comm)

SET(CMAKE_C_COMPILER "gcc")
SET(CMAKE_CXX_COMPILER "g++")
SET(CMAKE_CXX_FLAGS_DEBUG "$ENV{CXXFLAGS} -O0 -Wall -g -ggdb")
SET(CMAKE_CXX_FLAGS_RELEASE "$ENV{CXXFLAGS} -O3 -Wall -DNDEBUG")
SET(CMAKE_THREAD_PREFER_PTHREAD 1)

SET(LIBRARY_OUTPUT_PATH ${CMAKE_INSTALL_PREFIX}/lib)
SET(FILES_OUTPUT_PATH ${CMAKE_INSTALL_PREFIX}/include/rdp-comm)

FIND_PATH(GFLAGS_INCLUDE_DIR gflags/gflags.h
  HINTS "/usr/include/" "/usr/local/include/" ${RDP_DEPS}/gflags/include/)
FIND_LIBRARY(GFLAGS_LIBRARY NAMES gflags
  HINTS "/usr/lib/" "/usr/local/lib/" ${RDP_DEPS}/gflags/lib/)

FIND_PATH(GLOG_INCLUDE_DIR glog/logging.h
  HINTS "/usr/include/" "/usr/local/include/" ${RDP_DEPS}/glog/include/)
FIND_LIBRARY(GLOG_LIBRARY NAMES glog
  HINTS "/usr/lib/" "/usr/local/lib/" ${RDP_DEPS}/glog/lib/)

FIND_PATH(JANSSON_INCLUDE_DIR jansson.h
  HINTS "/usr/include/" "/usr/local/include/" ${RDP_DEPS}/jansson/include/)
FIND_LIBRARY(JANSSON_LIBRARY NAMES jansson
  HINTS "/usr/lib/" "/usr/local/lib/" ${RDP_DEPS}/jansson/lib/)

FIND_PATH(ZOOKEEPER_INCLUDE_DIR zookeeper/zookeeper.h
  HINTS "/usr/include/" "/usr/local/include/" ${RDP_DEPS}/zklib/include/)
FIND_LIBRARY(ZOOKEEPER_LIBRARY NAMES zookeeper_mt
  HINTS "/usr/lib/" "/usr/local/lib/" ${RDP_DEPS}/zklib/lib/)

FIND_PATH(GMOCK_INCLUDE_DIR gmock/gmock.h
  HINTS "/usr/include/" "/usr/local/include/" ${RDP_DEPS}/gtest/include/)
FIND_LIBRARY(GMOCK_LIBRARY NAMES gmock gmock_main
  HINTS "/usr/lib/" "/usr/local/lib/" ${RDP_DEPS}/gtest/lib/)

FIND_PATH(GTEST_INCLUDE_DIR gtest/gtest.h
  HINTS "/usr/include/" "/usr/local/include/" ${RDP_DEPS}/gtest/include/)
FIND_LIBRARY(GTEST_LIBRARY NAMES gtest gtest_main
  HINTS "/usr/lib/" "/usr/local/lib/" ${RDP_DEPS}/gtest/lib/)

FIND_PATH(BOOST_INCLUDE_DIR boost/algorithm/string.hpp
  HINTS "/usr/include/" "/usr/local/include/" ${RDP_DEPS}/boost/include/)
FIND_LIBRARY(BOOST_SYS_LIBRARY NAMES boost_system
  HINTS "/usr/lib/" "/usr/local/lib/" ${RDP_DEPS}/boost/lib/)
FIND_LIBRARY(BOOST_THD_LIBRARY NAMES boost_thread
  HINTS "/usr/lib/" "/usr/local/lib/" ${RDP_DEPS}/boost/lib/)

FIND_PATH(RDKAFKA_INCLUDE_DIR librdkafka/rdkafkacpp.h
  HINTS "/usr/include/" "/usr/local/include/" ${RDP_DEPS}/librdkafka/include/)
FIND_LIBRARY(RDKAFKA_LIBRARY NAMES rdkafka++
  HINTS "/usr/lib/" "/usr/local/lib/" ${RDP_DEPS}/librdkafka/lib/)

SET(SRC_LIST
  zk_process.cc
  zk_process.h
  leader_election.cc
  leader_election.h
  threadbase.cc
  threadbase.h
  signal_handler.cc
  signal_handler.h
  util.cc
  util.h
  comm_incl.cc
  comm_incl.h
  config.cc
  config.h
  zk_config.cc
  zk_config.h
  metric_reporter.cc
  fiu_engine.h
  fiu_engine.cc
  kafka_consumer.h
  kafka_consumer.cc
  logger.h
  logger.cc
  file_mgt.h
  file_mgt.cc
)

INCLUDE_DIRECTORIES(${GFLAGS_INCLUDE_DIR})
MESSAGE(STATUS ${GFLAGS_INCLUDE_DIR}, ${GFLAGS_LIBRARY})

INCLUDE_DIRECTORIES(${GLOG_INCLUDE_DIR})
MESSAGE(STATUS ${GLOG_INCLUDE_DIR}, ${GLOG_LIBRARY})

INCLUDE_DIRECTORIES(${JANSSON_INCLUDE_DIR})
MESSAGE(STATUS ${JANSSON_INCLUDE_DIR}, ${JANSSON_LIBRARY})

INCLUDE_DIRECTORIES(${ZOOKEEPER_INCLUDE_DIR})
MESSAGE(STATUS ${ZOOKEEPER_INCLUDE_DIR}, ${ZOOKEEPER_LIBRARY})

INCLUDE_DIRECTORIES(${GMOCK_INCLUDE_DIR})
MESSAGE(STATUS ${GMOCK_INCLUDE_DIR}, ${GMOCK_LIBRARY})

INCLUDE_DIRECTORIES(${GTEST_INCLUDE_DIR})
MESSAGE(STATUS ${GTEST_INCLUDE_DIR}, ${GTEST_LIBRARY})

INCLUDE_DIRECTORIES(${BOOST_INCLUDE_DIR})
MESSAGE(STATUS ${BOOST_INCLUDE_DIR}, ${BOOST_LIBRARY})

INCLUDE_DIRECTORIES(${RDKAFKA_INCLUDE_DIR})
MESSAGE(STATUS ${RDKAFKA_INCLUDE_DIR}, ${RDKAFKA_LIBRARY})

ADD_LIBRARY(rdp-comm SHARED
  ${SRC_LIST}
)

ADD_LIBRARY(rdp-comm-static STATIC
  ${SRC_LIST}
)

SET_TARGET_PROPERTIES(rdp-comm-static PROPERTIES OUTPUT_NAME "rdp-comm")

SET_TARGET_PROPERTIES(rdp-comm PROPERTIES CLEAN_DIRECT_OUTPUT 1)

SET_TARGET_PROPERTIES(rdp-comm-static PROPERTIES CLEAN_DIRECT_OUTPUT 1)

INSTALL(TARGETS rdp-comm rdp-comm-static
  LIBRARY DESTINATION ${LIBRARY_OUTPUT_PATH}
  ARCHIVE DESTINATION ${LIBRARY_OUTPUT_PATH})

INSTALL(FILES
  comm_incl.h
  leader_election.h
  signal_handler.h
  threadbase.h
  util.h
  zk_process.h
  config.h
  zk_config.h
  metric_reporter.h
  fiu_engine.h
  logger.h
  kafka_consumer.h
  file_mgt.h
  DESTINATION ${FILES_OUTPUT_PATH})

SET(TEST_SRC_FILE
  test/leader_election_test.h
  test/leader_election_test.cc
  test/main.cc
  test/rdp_environment.h
  test/rdp_environment.cc
  test/util_test.h
  test/util_test.cc
  test/zkprocess_test.h
  test/zkprocess_test.cc
  test/zk_config_test.h
  test/zk_config_test.cc
  )

ADD_EXECUTABLE(rdp-comm-test ${TEST_SRC_FILE} ${SRC_LIST})


TARGET_LINK_LIBRARIES(rdp-comm-test
  pthread
  curl
  ${GFLAGS_LIBRARY}
  ${GLOG_LIBRARY}
  ${JANSSON_LIBRARY}
  ${ZOOKEEPER_LIBRARY}
  ${GMOCK_LIBRARY}
  ${GTEST_LIBRARY}
  ${BOOST_SYS_LIBRARY}
  ${BOOST_THD_LIBRARY}
  ${RDKAFKA_LIBRARY}
  rt
)

