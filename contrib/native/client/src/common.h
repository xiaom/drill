#ifndef _COMMON_H_
#define _COMMON_H_

#include <string>
#include <cassert>
#include <vector>
#include <cstdio>
#include <iostream>
#include <fstream>
#include <cstring>

#include <boost/cstdint.hpp>
#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/thread.hpp>
#include <google/protobuf/message_lite.h>
#include <google/protobuf/wire_format_lite.h>

#include "proto-cpp/GeneralRPC.pb.h"
#include "proto-cpp/SchemaDef.pb.h"
#include "proto-cpp/UserBitShared.pb.h"
#include "proto-cpp/User.pb.h"
#include "proto-cpp/ExecutionProtos.pb.h"

typedef std::vector<boost::uint8_t> DataBuf;

using std::cout;
using std::cerr;
using std::endl;
using std::string;
using std::ifstream;
using std::ostream;
using std::memmove;
using exec::rpc::RpcMode;

namespace asio = boost::asio;

typedef void MQueryResult; // TODO expand later
typedef void QueryResultHandle;

#define EXTRA_DEBUGGING false
#define CODER_DEBUGGING false
#define LENGTH_PREFIX_MAX_LENGTH 4
namespace Drill {

struct UserServerEndPoint {
    string m_addr;
    int m_port;
    UserServerEndPoint(string addr, int port):m_addr(addr),m_port(port) { }
};
} // namespace Drill
#endif

