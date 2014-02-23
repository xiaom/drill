#ifndef RPC_MESSAGE_H
#define RPC_MESSAGE_H
#include "common.h"
#include <google/protobuf/message_lite.h>

namespace Drill {

class InBoundRpcMessage {
  public:
    RpcMode m_mode;
    int m_rpc_type;
    int m_coord_id;
    DataBuf m_pbody;
    DataBuf m_dbody;
    friend ostream& operator<< (ostream & out, InBoundRpcMessage& msg);

    /*
    InBoundRpcMessage(RpcMode mode,
    	int rpc_type, int coord_id, DataBuf pbody, vector<DataBuf>  dbody):
        m_mode(mode), m_rpc_type(rpc_type), m_coord_id(coord_id), m_pbody(pbody), m_dbody(dbody) { }
    */

    // GetBodySize
    // ToString
    // get ProtobufBodyAsIs

};

class OutBoundRpcMessage {
  public:
    RpcMode m_mode;
    int m_rpc_type;
    int m_coord_id;
    const google::protobuf::MessageLite* m_pbody;

    OutBoundRpcMessage(RpcMode mode, int rpc_type, int coord_id, const google::protobuf::MessageLite* pbody):
        m_mode(mode), m_rpc_type(rpc_type), m_coord_id(coord_id), m_pbody(pbody) { }
    friend ostream& operator<< (ostream & out, OutBoundRpcMessage& msg);
    // GetBodySize
    // GetRawBodySize
    // ToString
};
/*
inline ostream& operator<< (ostream & out, InBoundRpcMessage & msg) {

    out << "InboundRpcMessage [pBody= (" << msg.m_pbody.size() << " bytes)"  \
        << ", mode=" << msg.m_mode  << ", rpcType=" << msg.m_rpc_type  << ", coordinationId=" << msg.m_coord_id \
        << ", dBodies= (" << msg.m_dbody.size() << " bytes)" \
        << "]" ;
    return out;
}
inline ostream& operator<< (ostream & out, OutBoundRpcMessage & msg) {
    out << "OutboundRpcMessage [pBody= (" << msg.m_pbody->ByteSize() << " bytes)"\
        << ", mode=" << msg.m_mode  << ", rpcType=" << msg.m_rpc_type  << ", coordinationId=" << msg.m_coord_id \
        // << ", dBodies=" << m_dBodies
        << "]";
    return out;
}
*/
}

#endif
