#include "common.h"
#include "drill-client-async.h"
using boost::system::error_code;
using Drill::DrillClientAsync;
using Drill::UserServerEndPoint;
using namespace exec::user;

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
        cerr << "Invalid rpc version.  Expected << " <<u2b.rpc_version() << ", actual "<< b2u.rpc_version() << "." << endl;
        return false;
    }
    return true;
}


void DrillClientAsync::SubmitQuery(QueryType t, const string& plan) {
    cerr << "plan = " << plan << endl;
    RunQuery query;
    query.set_results_mode(STREAM_FULL);
    query.set_type(t);
    query.set_plan(plan);

    int coord_id = 133;
    OutBoundRpcMessage out_msg(exec::rpc::REQUEST, RUN_QUERY, coord_id, &query);
    send_sync(out_msg);

    for (int i =0 ; i< 6000000; i++) {
        ;
    }

    do_read();
}

QueryResultHandle DrillClientAsync::GetResult() {

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


void DrillClientAsync::do_read() {
    // read at most 4 bytes to get the length of the message
    cerr << "do read" << endl;
    async_read(m_socket,
               asio::buffer(m_rbuf.data(), 4),
               boost::bind(&DrillClientAsync::handle_read_length, this,
                           asio::placeholders::error, asio::placeholders::bytes_transferred)
              );
}
void DrillClientAsync::handle_read_length(const error_code & err, size_t bytes_transferred) {

    if (!err) {
        cerr << "> handle read length" << endl;
        int bytes_read = m_decoder.LengthDecode(m_rbuf.data(), &m_rmsg_len);
        cerr << "bytes read = " << bytes_read << endl;
        cerr << "m_rmsg_len = " << m_rmsg_len << endl;

        if (m_rmsg_len) {
            size_t leftover = LENGTH_PREFIX_MAX_LENGTH - bytes_read;
            if(leftover) {
                memmove(m_rbuf.data(), m_rbuf.data() + bytes_read, leftover);
            }
            async_read( m_socket,
                        asio::buffer(m_rbuf.data() + leftover, m_rmsg_len - leftover),
                        boost::bind(&DrillClientAsync::handle_read_msg, this,
                                    asio::placeholders::error, asio::placeholders::bytes_transferred)
                      );

        }
    } else {
        cerr << "handle_read_length error: " << err << "\n";
    }
}
void DrillClientAsync::handle_read_msg(const error_code & err, size_t bytes_transferred) {
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

        exec::shared::QueryId qid;
        cerr << "m_pbody = " << msg.m_pbody.size() << endl;
        qid.ParseFromArray(msg.m_pbody.data(), msg.m_pbody.size());
        cerr << qid.DebugString() << endl;

    } else {
        cerr << "Error: " << err << "\n";
    }

}

