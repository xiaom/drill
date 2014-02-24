#include "common.h"
#include "drill-client-sync.h"
using boost::system::error_code;
using Drill::DrillClientSync;
using Drill::UserServerEndPoint;
using namespace exec::user;

void DrillClientSync::Connect(const UserServerEndPoint& userver) {
    // connect the endpoint
    asio::ip::tcp::endpoint endpoint(asio::ip::address::from_string(userver.m_addr), userver.m_port);
    m_socket.connect(endpoint);
}

bool DrillClientSync::ValidateHandShake() {
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

void DrillClientSync::SubmitQuerySync(QueryType t, const string& plan) {
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

MQueryResult DrillClientSync::GetResultSync() {

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

void DrillClientSync::send_sync(OutBoundRpcMessage& msg) {
    m_encoder.Encode(m_wbuf, msg);
    m_socket.write_some(asio::buffer(m_wbuf));
}

void DrillClientSync::recv_sync(InBoundRpcMessage& msg) {
    m_socket.read_some(asio::buffer(m_rbuf));
    uint32_t length = 0;
    int bytes_read = m_decoder.LengthDecode(m_rbuf.data(), &length);
    m_decoder.Decode(m_rbuf.data() + bytes_read, length, msg);
}

