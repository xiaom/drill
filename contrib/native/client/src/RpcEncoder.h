#ifndef RPC_ENCODEER_H
#define RPC_ENCODEER_H

#include <google/protobuf/message_lite.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include "common.h"
#include "RpcMessage.h"

using exec::rpc::RpcHeader;
using google::protobuf::io::CodedOutputStream;
using google::protobuf::io::ArrayOutputStream;


class RpcEncoder {
public:
    RpcEncoder() {}
    ~RpcEncoder() { }
    bool Encode(DataBuf& buf,OutBoundRpcMessage& msg);
};

bool RpcEncoder::Encode(DataBuf& buf, OutBoundRpcMessage& msg)
{
    // Todo:
    //
    // - let a context manager to allocate a buffer `ByteBuf buf = ctx.alloc().buffer();`
    // - builder pattern
    //
    cerr << "\nEncoding outbound message " << msg << endl;

    RpcHeader header;
    header.set_mode(msg.m_mode);
    header.set_coordination_id(msg.m_coord_id);
    header.set_rpc_type(msg.m_rpc_type);

    // calcute the length of the message
    int header_length = header.ByteSize();
    int proto_body_length = msg.m_pbody->ByteSize();
    int full_length = HEADER_TAG_LENGTH + getRawVarintSize(header_length) + header_length + \
                      PROTOBUF_BODY_TAG_LENGTH + getRawVarintSize(proto_body_length) + proto_body_length;

    /*
    if(raw_body_length > 0) {
        full_length += (RAW_BODY_TAG_LENGTH + getRawVarintSize(raw_body_length) + raw_body_length);
    }
    */

    buf.resize(full_length + getRawVarintSize(full_length));
    ArrayOutputStream* os = new ArrayOutputStream(buf.data(), buf.size());
    CodedOutputStream* cos = new CodedOutputStream(os);


    cerr << "Writing full length " << full_length << endl;

    // write full length first (this is length delimited stream).
    cos->WriteVarint32(full_length);

    cerr << "Writing header length " << header_length << endl;
    cos->WriteVarint32(HEADER_TAG);
    cos->WriteVarint32(header_length);


    header.SerializeToCodedStream(cos);

    // write protobuf body length and body
    cerr << "Writing protobuf body length " << proto_body_length << endl;


    cos->WriteVarint32(PROTOBUF_BODY_TAG);
    cos->WriteVarint32(proto_body_length);
    msg.m_pbody->SerializeToCodedStream(cos);


    /*
    not needed for client
    // if exists, write data body and tag.
    if(msg.getRawBodySize() > 0) {
        //if(RpcConstants.EXTRA_DEBUGGING) logger.debug("Writing raw body of size {}", msg.getRawBodySize());
        cerr << "Writing raw body of size {}" <<  msg.getRawBodySize() << endl;

        cos->WriteVarint32(RAW_BODY_TAG);
        cos->WriteVarint32(raw_body_length);

        // Todo: not sure how to use it in C++
        // need to flush so that dbody goes after if cos is caching.
        //
        // cos->flush();

        CompositeByteBuf cbb = new CompositeByteBuf(buf.alloc(), true, msg.dBodies.length + 1);
        cbb.addComponent(buf);
        int bufLength = buf.readableBytes();
        for(ByteBuf b : msg.dBodies) {
            cbb.addComponent(b);
            bufLength += b.readableBytes();
        }
        cbb.writerIndex(bufLength);
        out.add(cbb);
    }
    */

    return true;
}



#endif
