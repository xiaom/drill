#ifndef DRILL_CLIENT_H
#define DRILL_CLIENT_H
#include "common.h"
#include "RpcEncoder.h"
#include "RpcDecoder.h"
#include "RpcMessage.h"
#include <google/protobuf/message.h>
//#include <boost/asio/use_future.hpp>
//#include <future/thread/future.hpp>

// Notes:
//
// - vector::data() is in C++ 11 but works fine with Visual Studio 2010

using namespace boost::system;
using namespace exec::user;
using boost::system::error_code; 

typedef void MQueryResult; // TODO expand later
typedef void QueryResultHandle;

/*
class QueryResultListener{
    SubmissionFailed();
    ResultArrived();
    Await();
    QueryIdArrived();
    GetResults();
}
*/

#define LENGTH_PREFIX_MAX_LENGTH 4
class DrillClient {

public:
    // TODO:
    // 
    // - add DrillConfig to configure client
    //      - reconncection_times (as in ExecConstants.BIT_RETRY_TIMES)
    //      - reconncection_delay (as in ExecConstants.BIT_RETRY_DELAY)
    // - buffer allocator
    // 
    // memeber functions
    //
    // bool Active()
    // FinalizeConnection() : handshake the connection
    // Connect(): connect via zookeeper
    // 
    // ZKClusterCoordinator
    //  
    // - Start(uint64_t mills_to_wait): 
    //      : millis to wait before returning erors if client coordination service is not started 
    //      : use 0 to wait indefinitely
    // - GetAvailableEndpoints():
    //      

    DrillClient(asio::io_service& io_service):m_io_service(io_service),m_socket(io_service), m_rbuf(10240), m_wbuf(10240), m_rmsg_len(0) { };

    ~DrillClient() { };

    // connects the client to a Drillbit UserServer
    void Connect(const UserServerEndPoint& endpoint);

    // FIXME
    bool Active(){
        return true;
    }

    // FIXME 
    // 
    // Handle exception, we need to reconnect if sumbission failed
    bool Reconnect(){
        if (Active()) return true;

        /*
        int retry = m_reconnect_times;
        while( retry > 0 ){
            retry--;
            // ask the cluster coordinator for available drillbit

        }
        */
        return true;
    }

    void Close() { m_socket.close(); };
    void CloseAsync(){
        m_io_service.post(boost::bind(&DrillClient::Close, this));
    }
    bool ValidateHandShake(); // throw expection if not valid

    QueryResultHandle SubmitQuery(QueryType t, const string& plan);
    QueryResultHandle GetResult();
    void SubmitQuerySync(QueryType t, const string& plan);
    MQueryResult GetResultSync();
    
private:
    // future<QueryResult> f_results; 
    asio::io_service& m_io_service;
    asio::ip::tcp::socket m_socket;
    RpcEncoder m_encoder;
    RpcDecoder m_decoder;
    
    // TODO use boost::circular_buffer or streambuf?
    DataBuf m_rbuf; // buffer for receiving message
    DataBuf m_wbuf; // buffer for sending message
    uint32_t m_rmsg_len;
    bool m_last_chunk;
    
    /*    
    void do_read(){
        // read at most 4 bytes to get the length of the message
        cerr << "do read" << endl;
        async_read(m_socket, 
                asio::buffer(m_rbuf.data(), LENGTH_PREFIX_MAX_LENGTH), 
                    boost::bind(&DrillClient::handle_read_length, this, 
                        asio::placeholders::error, asio::placeholders::bytes_transferred)
            );
    }
    void handle_read_length(const error_code & err, size_t bytes_transferred)
    {
        cerr << "read length" << endl;
        int bytes_read = m_decoder.LengthDecode(m_rbuf.data(), &m_rmsg_len);

        if (!err && m_rmsg_len){
            size_t leftover = LENGTH_PREFIX_MAX_LENGTH - bytes_read;
            if(leftover){
                memmove(m_rbuf.data(), m_rbuf.data() + bytes_read, leftover); 
            }
            async_read(m_socket, 
                    asio::buffer(m_rbuf.data() + leftover, m_rmsg_len - leftover), 
                        boost::bind(&DrillClient::handle_read_msg, this, 
                            asio::placeholders::error, asio::placeholders::bytes_transferred)
                    );

        }
        else{
            cerr << "Error: " << err << "\n";
        }
    }
    void handle_read_msg(const error_code & err, size_t bytes_transferred)
    {
        cerr << "read msg" << endl;
        if (!err) {
            // now message is read into m_rbuf with length m_rmsg_len

            QueryResult result;
            InBoundRpcMessage msg;
            m_decoder.Decode(m_rbuf.data(), m_rmsg_len, msg);
            m_rmsg_len = 0; // reset the length
            // todo ... handle the chunk
            result.ParseFromArray(msg.m_pbody.data(), msg.m_pbody.size());
            cerr << result.DebugString() << endl;
 
        } else{
            cerr << "Error: " << err << "\n";
        }

    }
*/
    void send_sync(OutBoundRpcMessage& msg)
    {
        m_encoder.Encode(m_wbuf, msg);
        m_socket.write_some(asio::buffer(m_wbuf));
    }

    void recv_sync(InBoundRpcMessage& msg)
    {
        m_socket.read_some(asio::buffer(m_rbuf));
        uint32_t length = 0;
        int bytes_read = m_decoder.LengthDecode(m_rbuf.data(), &length);
        m_decoder.Decode(m_rbuf.data() + bytes_read, length, msg);
    }
};

void DrillClient::Connect(const UserServerEndPoint& userver)
{
    // connect the endpoint
    asio::ip::tcp::endpoint endpoint(asio::ip::address::from_string(userver.m_addr), userver.m_port);
    m_socket.connect(endpoint);
}

bool DrillClient::ValidateHandShake()
{
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
        cerr << "Invalid rpc version.  Expected << " <<u2b.rpc_version() << ", actual "<< b2u.rpc_version() << "." << endl;
        return false;
    }
    return true;
}

void DrillClient::SubmitQuerySync(QueryType t, const string& plan)
{
    cerr << "plan = " << plan << endl;
    RunQuery query;
    query.set_results_mode(STREAM_FULL);
    query.set_type(t);
    query.set_plan(plan);

    int coord_id = 1;
    OutBoundRpcMessage out_msg(exec::rpc::REQUEST, RUN_QUERY, coord_id, &query);
    send_sync(out_msg);

    InBoundRpcMessage in_msg;
    recv_sync(in_msg);
    exec::shared::QueryId qid;
    cerr << "m_pbody = " << in_msg.m_pbody.size() << endl;
    qid.ParseFromArray(in_msg.m_pbody.data(), in_msg.m_pbody.size());

    cerr << qid.DebugString() << endl;
    
}

MQueryResult DrillClient::GetResultSync()
{

    std::vector<InBoundRpcMessage> r_msgs(1024);
    QueryResult result;
    int cnt = 0;
    do {
        cerr << "--> Receiving msg " << cnt + 1 << ": " << endl;
        recv_sync(r_msgs[cnt]);
        result.ParseFromArray(r_msgs[cnt].m_pbody.data(), r_msgs[cnt].m_pbody.size());
        cerr << result.DebugString() << endl;
        cnt ++;
    } while(!result.is_last_chunk());

    cerr << cnt + 1 << " messages received for results" << endl;


    exec::rpc::Ack ack;
    ack.set_ok(true);
    int coord_id = r_msgs[cnt-1].m_coord_id + 1;
    OutBoundRpcMessage ack_msg(exec::rpc::RESPONSE, ACK, coord_id, &ack);
    send_sync(ack_msg);
}

#endif
