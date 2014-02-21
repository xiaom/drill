cmake_minimum_required(VERSION 2.8)

project(drillclient)

message("CMAKE_SOURCE_DIR = " ${CMAKE_SYSTEM_NAME})
#set(THIRD_PARTY_DIR ${CMAKE_SOURCE_DIR}/thirdparty)
#set(BOOST_ROOT ${THIRD_PARTY_DIR}/boost)
set(Boost_USE_STATIC_LIBS OFF) 
set(Boost_USE_MULTITHREADED ON)  
set(Boost_USE_STATIC_RUNTIME OFF) 
find_package(Boost REQUIRED COMPONENTS regex system date_time chrono thread)
include_directories(${Boost_INCLUDE_DIRS})

if(CMAKE_COMPILER_IS_GNUCXX) 
    set(CMAKE_EXE_LINKER_FLAGS "-lsqlite3 -lrt -lpthread")                                                   
endif()    

find_package(Protobuf REQUIRED)
#set(PROTOBUF_INCLUDE_DIR ${THIRD_PARTY_DIR}/protobuf/include)
#set(PROTOBUF_LIBRARY_DIR ${THIRD_PARTY_DIR}/protobuf/lib)
include_directories(${PROTOBUF_INCLUDE_DIR})
#link_directories(${PROTOBUF_LIBRARY_DIR})
message("PROTOBUF_INCLUDE_DIR = " ${PROTOBUF_INCLUDE_DIR})

include_directories(${CMAKE_SOURCE_DIR}/src)
file(GLOB ProtoCpp src/proto-cpp/*.cc)
message("ProtoCpp = " ${ProtoCpp})
add_executable(sync_client example/sync_client.cpp ${ProtoCpp}) 
target_link_libraries(sync_client ${Boost_LIBRARIES} protobuf)
