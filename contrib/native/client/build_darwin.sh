#!/bin/sh
# You need to configure
#
# - P4ROOT in your shell profile
#
 
# -------------------------
#export SIMBAENGINE_DIR=$P4ROOT/SimbaEngine/Maintenance/9.1/Product
#export DRIVERSUPPORT_DIR=$P4ROOT/Drivers/DriverSupport/Development/Dev01
#export DRIVERSHARED_DIR=$P4ROOT/Drivers/DriverShared
 
export THIRDPARTY_DIR=/usr/local/Cellar
export BOOST_DIR=$THIRDPARTY_DIR/boost/1.55.0_1
export PROTOBUF_DIR=$THIRDPARTY_DIR/protobuf/2.5.0
#export ZK_DIR=$P4ROOT/Drivers/Dirac/Development/Dev02/Product/ThirdParty/Zookeeper/Darwin_universal
export ZK_DIR=$THIRDPARTY_DIR/zookeeper/3.4.6
#export ZK_DIR=/home/xiaom/zk
 
#"-DCMAKE_OSX_ARCHITECTURES=x86_64;i386" \
mkdir -p build && cd build
cmake -D Boost_DEBUG=ON -D Boost_USE_STATIC_LIBS=ON \
    -D CMAKE_BUILD_TYPE=Debug \
    -D "CMAKE_OSX_ARCHITECTURES=x86_64" \
    -D PROTOBUF_INCLUDE_DIR=$PROTOBUF_DIR/include \
    -D PROTOBUF_LIBRARY=$PROTOBUF_DIR/lib/libprotobuf.a \
    -D Boost_INCLUDE_DIR=$BOOST_DIR/include \
    -D BOOST_LIBRARYDIR=$BOOST_DIR/lib \
    -D Zookeeper_INCLUDE_DIR=$ZK_DIR/include \
    -D Zookeeper_LIBRARY=$ZK_DIR/lib/libzookeeper_st.a \
    ..
 
echo "----------------"
echo "Start Make"
echo "----------------"
make drillClient
