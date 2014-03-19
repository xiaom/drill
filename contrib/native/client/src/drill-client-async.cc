#include "common.h"
#include "drill-client-async.h"
#include "recordBatch.h"

using boost::system::error_code;
using namespace exec::user;
using namespace Drill;

void DrillClientAsync::Connect(const UserServerEndPoint& userver) {
    // connect the endpoint
    asio::ip::tcp::endpoint endpoint(asio::ip::address::from_string(userver.m_addr), userver.m_port);
    m_socket.connect(endpoint);
}
void DrillClientAsync::send_sync(OutBoundRpcMessage& msg) {
    m_encoder.Encode(m_wbuf, msg);
    m_socket.write_some(asio::buffer(m_wbuf));
}

void DrillClientAsync::recv_sync(InBoundRpcMessage& msg) {
    m_socket.read_some(asio::buffer(m_rbuf));
    uint32_t length = 0;
    int bytes_read = m_decoder.LengthDecode(m_rbuf.data(), &length);
    m_decoder.Decode(m_rbuf.data() + bytes_read, length, msg);
}

bool DrillClientAsync::ValidateHandShake() {
    UserToBitHandshake u2b;
    u2b.set_channel(exec::shared::USER);
    u2b.set_rpc_version(1);
    u2b.set_support_listening(true);

    int coord_id = 1;

    OutBoundRpcMessage out_msg(exec::rpc::REQUEST, HANDSHAKE, coord_id, &u2b);
    send_sync(out_msg);

    InBoundRpcMessage in_msg;
    recv_sync(in_msg);

    BitToUserHandshake b2u;
    b2u.ParseFromArray(in_msg.m_pbody.data(), in_msg.m_pbody.size());

    // validate handshake
    if (b2u.rpc_version() != u2b.rpc_version()) {
        BOOST_LOG_TRIVIAL(trace) << "Invalid rpc version.  Expected << " <<u2b.rpc_version() << ", actual "<< b2u.rpc_version() << "." ;
        return false;
    }
    return true;
}


void DrillClientAsync::SubmitQuery(QueryType t, const string& plan) {
    BOOST_LOG_TRIVIAL(trace) << "plan = " << plan;
    RunQuery query;
    query.set_results_mode(STREAM_FULL);
    query.set_type(t);
    query.set_plan(plan);

    int coord_id = 133;
    OutBoundRpcMessage out_msg(exec::rpc::REQUEST, RUN_QUERY, coord_id, &query);
    send_sync(out_msg);

    BOOST_LOG_TRIVIAL(trace)  << "do read";
    InBoundRpcMessage in_msg;
    recv_sync(in_msg);
    exec::shared::QueryId qid;
    BOOST_LOG_TRIVIAL(trace)  << "m_pbody = " << in_msg.m_pbody.size();
    qid.ParseFromArray(in_msg.m_pbody.data(), in_msg.m_pbody.size());
    BOOST_LOG_TRIVIAL(trace) << qid.DebugString();

}


void DrillClientAsync::GetResult() {
    // TODO: Check for last chunk and stop listening for more results
    this->m_last_chunk=false;
    BOOST_LOG_TRIVIAL(trace)  << "Getting Results" << endl;
    GetNextRecordBatch();
}

//TODO: GetNextRecordBatch should return a RecordBatch object thru a callback
void DrillClientAsync::GetNextRecordBatch() {
    BOOST_LOG_TRIVIAL(trace) << "Getting next record batch" << endl;
    //memset(m_readLengthBuf, 0, LEN_PREFIX_BUFLEN);
    async_read( 
	    m_socket,
	    asio::buffer(m_readLengthBuf, LEN_PREFIX_BUFLEN),
	    boost::bind(
            &DrillClientAsync::handle_read_length, 
            this,
            asio::placeholders::error, asio::placeholders::bytes_transferred)
        );
}

void DrillClientAsync::handle_read_length(const error_code & err, size_t bytes_transferred) {
    if (!err){
        BOOST_LOG_TRIVIAL(trace) << "> handle read length" << endl;
        int bytes_read = m_decoder.LengthDecode(this->m_readLengthBuf, &this->m_rmsg_len);
        BOOST_LOG_TRIVIAL(trace) << "bytes read = " << bytes_read << endl;
        BOOST_LOG_TRIVIAL(trace) << "m_rmsg_len = " << m_rmsg_len << endl;

        if(m_rmsg_len){
            size_t leftover = LEN_PREFIX_BUFLEN - bytes_read;
            // Allocate a buffer
            this->m_currentBuffer=allocateBuffer(m_rmsg_len);
            if(this->m_currentBuffer==NULL){
                //TODO: Throw out of memory exception
            }
            if(leftover){
                memcpy(m_currentBuffer, this->m_readLengthBuf + bytes_read, leftover);
            }
            this->m_dataBuffers.push_back(m_currentBuffer);
            async_read( m_socket,
                        asio::buffer(m_currentBuffer + leftover, m_rmsg_len - leftover),
                        boost::bind(&DrillClientAsync::handle_read_msg, this,
                                    asio::placeholders::error, asio::placeholders::bytes_transferred)
                       );
        }
    }else{
        BOOST_LOG_TRIVIAL(trace) << "handle_read_length error: " << err << "\n";
    }
}

void DrillClientAsync::handle_read_msg(const error_code & err, size_t bytes_transferred) {
    BOOST_LOG_TRIVIAL(trace) << "read msg" << endl;
    if (!err) {
        BOOST_LOG_TRIVIAL(trace) << "Data Message: bytes read = " << bytes_transferred << endl;

        InBoundRpcMessage msg;
        m_decoder.Decode(m_currentBuffer, m_rmsg_len, msg);
        BOOST_LOG_TRIVIAL(trace) << "Done decoding chunk" << endl;
        
        QueryResult qr;
        if(msg.m_rpc_type==QUERY_RESULT){
            qr.ParseFromArray(msg.m_pbody.data(), msg.m_pbody.size());
            BOOST_LOG_TRIVIAL(trace) << qr.DebugString();
            //TODO: Check QueryResult.queryState. QueryResult could have an error.
            //TODO: Build Record Batch here 
            RecordBatch* pRecordBatch= new RecordBatch(qr.def(), qr.row_count(), msg.m_dbody);
            pRecordBatch->build();
            this->m_recordBatches.push_back(pRecordBatch);

            //TODO: Provide a callback that is called when a record batch is received
#if DEBUG 
            pRecordBatch->print(10); // print at most 10 rows per batch

#endif
            
            if(qr.is_last_chunk()){
                BOOST_LOG_TRIVIAL(trace) << "Received last batch.";
                return;
            }
        }else{
            //TODO:If not QUERY_RESULT, then do appropriate error handling
            BOOST_LOG_TRIVIAL(trace) << "QueryResult returned " << msg.m_rpc_type;
            return;
        }
        exec::rpc::Ack ack;
        ack.set_ok(true);
        OutBoundRpcMessage ack_msg(exec::rpc::RESPONSE, ACK, msg.m_coord_id, &ack);
        send_sync(ack_msg);
        BOOST_LOG_TRIVIAL(trace) << "ACK sent" << endl;
        GetNextRecordBatch();
    }else{
        BOOST_LOG_TRIVIAL(trace) << "Error: " << err << "\n";
    }
    return;
}

