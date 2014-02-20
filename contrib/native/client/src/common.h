#ifndef DIRAC_COMMON
#define DIRAC_COMMON

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
#include <google/protobuf/wire_format.h>

#include "proto-cpp/GeneralRPC.pb.h"
#include "proto-cpp/SchemaDef.pb.h"
#include "proto-cpp/UserBitShared.pb.h"
#include "proto-cpp/User.pb.h"
#include "proto-cpp/ExecutionProtos.pb.h"

typedef std::vector<boost::uint8_t> DataBuf;

using exec::rpc::RpcMode;
using std::cout;
using std::cerr;
using std::endl;
using std::string;
using std::ifstream;
using std::ostream;
using std::memmove;

namespace asio = boost::asio;

#define EXTRA_DEBUGGING 1

struct UserServerEndPoint{
    string m_addr;
    int m_port;
    UserServerEndPoint(string addr, int port):m_addr(addr),m_port(port){ }
};

/*
 * Global variable, should put into encoder
 */
int HEADER_TAG;
int PROTOBUF_BODY_TAG;
int RAW_BODY_TAG;
int HEADER_TAG_LENGTH;
int PROTOBUF_BODY_TAG_LENGTH;
int RAW_BODY_TAG_LENGTH;

inline int MakeTag(int fieldNumber, int wireType)
{
    return (fieldNumber << 3) | wireType;
}

inline int getRawVarintSize(unsigned int value)
{
    int count = 0;
    while (true) {
        if ((value & ~0x7F) == 0) {
            count++;
            return count;
        } else {
            count++;
            value >>= 7;
        }
    }
}

inline void make_tags()
{
    //using exec::rpc::CompleteRpcMessage;
    //using google::protobuf::internal::WireFormat;
    //
    // The wire format is composed of a sequence of tag/value pairs, each
    // of which contains the value of one field (or one element of a repeated
    // field).  Each tag is encoded as a varint.  The lower bits of the tag
    // identify its wire type, which specifies the format of the data to follow.
    // The rest of the bits contain the field number.  Each type of field (as
    // declared by FieldDescriptor::Type, in descriptor.h) maps to one of
    // these wire types.  Immediately following each tag is the field's value,
    // encoded in the format specified by the wire type.  Because the tag
    // identifies the encoding of this data, it is possible to skip
    // unrecognized fields for forwards compatibility.

    /*
      enum WireType {
        WIRETYPE_VARINT           = 0,
        WIRETYPE_FIXED64          = 1,
        WIRETYPE_LENGTH_DELIMITED = 2,
        WIRETYPE_START_GROUP      = 3,
        WIRETYPE_END_GROUP        = 4,
        WIRETYPE_FIXED32          = 5,
      };
    */
    HEADER_TAG = MakeTag(1, 2);
    PROTOBUF_BODY_TAG = MakeTag(2, 2);
    RAW_BODY_TAG = MakeTag(3, 2);
    HEADER_TAG_LENGTH = getRawVarintSize(HEADER_TAG);
    PROTOBUF_BODY_TAG_LENGTH = getRawVarintSize(PROTOBUF_BODY_TAG);
    RAW_BODY_TAG_LENGTH = getRawVarintSize(RAW_BODY_TAG);
}

#endif

