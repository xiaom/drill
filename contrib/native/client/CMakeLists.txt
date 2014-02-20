cmake_minimum_required(VERSION 2.6)

project(drillclient)

set(THIRD_PARTY_DIR ${CMAKE_SOURCE_DIR}/thirdparty)
set(BOOST_ROOT ${THIRD_PARTY_DIR}/boost)
set(Boost_USE_STATIC_LIBS OFF) 
set(Boost_USE_MULTITHREADED ON)  
set(Boost_USE_STATIC_RUNTIME OFF) 
find_package(Boost REQUIRED COMPONENTS regex system date_time chrono thread)
include_directories(${Boost_INCLUDE_DIRS})

set(PROTOBUF_INCLUDE_DIR ${THIRD_PARTY_DIR}/protobuf/include)
set(PROTOBUF_LIBRARY_DIR ${THIRD_PARTY_DIR}/protobuf/lib)
include_directories(${PROTOBUF_INCLUDE_DIR})
link_directories(${PROTOBUF_LIBRARY_DIR})
message("PROTOBUF_INCLUDE_DIR = " ${PROTOBUF_INCLUDE_DIR})
message("PROTOBUF_LIBRARY_DIR = " ${PROTOBUF_LIBRARY_DIR})

include_directories(${CMAKE_SOURCE_DIR}/src)
file(GLOB ProtoCpp src/proto-cpp/*.cc)
message("ProtoCpp = " ${ProtoCpp})
add_executable(sync_client example/sync_client.cpp ${ProtoCpp}) 
target_link_libraries(sync_client pthread rt ${Boost_LIBRARIES} protobuf)
