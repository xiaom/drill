#ifndef RPC_DECODEER_H
#define RPC_DECODEER_H
#include <google/protobuf/io/coded_stream.h>
#include "common.h"

#ifndef _WIN32_WINDOWS
#define _WIN32_WINDOWS
#endif
using google::protobuf::io::CodedInputStream;

class RpcDecoder {
public:
    RpcDecoder() { }
    ~RpcDecoder() { }
    // bool Decode(const DataBuf& buf);
    // bool Decode(const DataBuf& buf, InBoundRpcMessage& msg);
    int LengthDecode(const uint8_t* buf, uint32_t* length); // read the length prefix (at most 4 bytes)
    int Decode(const uint8_t* buf, int size, InBoundRpcMessage& msg);
};


// return the number of bytes we have read
int RpcDecoder::LengthDecode(const uint8_t* buf, uint32_t* p_length){
    // read the frame to get the length of the message and then
    
    CodedInputStream* cis = new CodedInputStream(buf, 4); // read 4 bytes at most
    
    int pos0 = cis->CurrentPosition(); // for debugging
    cis->ReadVarint32(p_length);
    int pos1 = cis->CurrentPosition();
    cerr << "Reading full length " << *p_length << endl;
    assert( (pos1-pos0) == getRawVarintSize(*p_length));

    return (pos1-pos0);
}

// TODO: error handling
//
// - assume that the entire message is in the buffer and the buffer is constrained to this message
// - easy to handle with raw arry in C++
int RpcDecoder::Decode(const uint8_t* buf, int length, InBoundRpcMessage& msg)
{

    // if(!ctx.channel().isOpen()){ return; }

    if (EXTRA_DEBUGGING)
        cerr <<  "\nInbound rpc message received." << endl;

    CodedInputStream* cis = new CodedInputStream(buf, length);


    int pos0 = cis->CurrentPosition(); // for debugging

    int len_limit = cis->PushLimit(length);

    /*
    About ExpectTag

    Usually returns true if calling ReadVarint32() now would produce the given value.

    Will always return false if ReadVarint32() would not return the given value. If ExpectTag() returns true, it also advances past the varint. For best performance, use a compile-time constant as the parameter. Always inline because this collapses to a small number of instructions when given a constant parameter, but GCC doesn't want to inline by default.
    */
    uint32_t header_length = 0;
    cis->ExpectTag(HEADER_TAG);
    cis->ReadVarint32(&header_length);
    cerr << "Reading header length " << header_length << ", post read index " << cis->CurrentPosition() << endl;

    RpcHeader header;
    int header_limit = cis->PushLimit(header_length);
    header.ParseFromCodedStream(cis);
    cis->PopLimit(header_limit);
    msg.m_mode = header.mode();
    msg.m_coord_id = header.coordination_id();
    msg.m_rpc_type = header.rpc_type();

    //if(RpcConstants.EXTRA_DEBUGGING) logger.debug(" post header read index {}", buffer.readerIndex());

    // read the protobuf body into a buffer.
    cis->ExpectTag(PROTOBUF_BODY_TAG);
    uint32_t p_body_length = 0;
    cis->ReadVarint32(&p_body_length);
    cerr << "Reading protobuf body length " << p_body_length << ", post read index " << cis->CurrentPosition() << endl;

    msg.m_pbody.resize(p_body_length);
    cis->ReadRaw(msg.m_pbody.data(),p_body_length);


    // read the data body.
    if (cis->BytesUntilLimit() > 0 ) {
        if(EXTRA_DEBUGGING)
            cerr << "Reading raw body, buffer has "<< cis->BytesUntilLimit() << " bytes available, current possion "<< cis->CurrentPosition()  << endl;
        cis->ExpectTag(RAW_BODY_TAG);
        uint32_t d_body_length = 0;
        cis->ReadVarint32(&d_body_length);

        if(cis->BytesUntilLimit() != d_body_length)
            cerr << "Expected to receive a raw body of " << d_body_length << " bytes but received a buffer with " <<cis->BytesUntilLimit() << " bytes." << endl;
        msg.m_dbody.resize(d_body_length);
        cis->ReadRaw(msg.m_dbody.data(), d_body_length);
        if(EXTRA_DEBUGGING) {
            cerr << "Read raw body of " << d_body_length << " bytes" << endl;
        }
    } else {
        if(EXTRA_DEBUGGING) cerr << "No need to read raw body, no readable bytes left." << endl;
    }
    cis->PopLimit(len_limit);


    // return the rpc message.
    // move the reader index forward so the next rpc call won't try to work with it.
    // buffer.skipBytes(dBodyLength);
    // messageCounter.incrementAndGet();
    cerr << "Inbound Rpc Message Decoded " << msg << endl;
    
    int pos1 = cis->CurrentPosition(); 
    assert((pos1-pos0) == length);
    return (pos1-pos0);
}

#endif