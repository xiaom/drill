cmake_minimum_required(VERSION 2.6)

project(drillclient)

set(Boost_USE_STATIC_LIBS OFF) 
set(Boost_USE_MULTITHREADED ON)  
set(Boost_USE_STATIC_RUNTIME OFF) 
find_package(Boost REQUIRED COMPONENTS regex system date_time chrono thread)
include_directories(${Boost_INCLUDE_DIRS})

set(PROTOBUF_INCLUDE_DIR ${CMAKE_SOURCE_DIR}/thirdparty/include)
set(PROTOBUF_LIBRARY_DIR ${CMAKE_SOURCE_DIR}/thirdparty/lib)
include_directories(${PROTOBUF_INCLUDE_DIR})
link_directories(${PROTOBUF_LIBRARY_DIR})
message("PROTOBUF_INCLUDE_DIR = " ${PROTOBUF_INCLUDE_DIR})
message("PROTOBUF_LIBRARY_DIR = " ${PROTOBUF_LIBRARY_DIR})

include_directories(${CMAKE_SOURCE_DIR}/src)
file(GLOB ProtoCpp ${CMAKE_SOURCE_DIR}  src/proto-cpp/*.cc)
add_executable(sync_client sync_client.cpp ${ProtoCpp}) 
target_link_libraries(client_sync pthread rt ${Boost_LIBRARIES} protobuf)
