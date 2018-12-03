CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
PROJECT(topic_trans)

#SET(CMAKE_BUILD_TYPE "Release")
SET(CMAKE_BUILD_TYPE "Debug")
SET(CMAKE_C_COMPILER "gcc")
SET(CMAKE_CXX_COMPILER "g++")
SET(CMAKE_CXX_FLAGS_DEBUG "$ENV{CXXFLAGS} -O0 -Wall -g -ggdb")
SET(CMAKE_CXX_FLAGS_RELEASE "$ENV{CXXFLAGS} -O3 -Wall -DNDEBUG")
execute_process(COMMAND realpath ../../deps
               OUTPUT_VARIABLE tmp_res)
STRING(REPLACE "\n" "" RDP_DEPS ${tmp_res})
message(${RDP_DEPS})               

LINK_DIRECTORIES(${RDP_DEPS}/librdp_message/lib/)

INCLUDE_DIRECTORIES(
  ${RDP_DEPS}/librdp_message/include/message/
  ${RDP_DEPS}/zklib/include/
  ${RDP_DEPS}/librdkafka/include/librdkafka/
  ${RDP_DEPS}/protobuf/include/
  ${RDP_DEPS}/jansson/include
  ${RDP_DEPS}/lz4/include
)

AUX_SOURCE_DIRECTORY(. TOOLS_LIST)

ADD_EXECUTABLE(generate_package
  main.cc json2pb.cc)

TARGET_LINK_LIBRARIES(generate_package
  rdp_message
  pthread
  ${RDP_DEPS}/protobuf/lib/libprotobuf.so
  ${RDP_DEPS}/zklib/lib/libzookeeper_mt.so
  ${RDP_DEPS}/librdkafka/lib/librdkafka++.so
  ${RDP_DEPS}/jansson/lib/libjansson.so
  ${RDP_DEPS}/lz4/lib/liblz4.so
)
