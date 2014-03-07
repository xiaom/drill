#include "common.h"
#include "drill-client.h"
using boost::system::error_code;
using std::vector;
using namespace Drill;


void DrillClientSync2::OpenSession(const UserServerEndPoint& userver, ExecutionContext& ctx) {
    // connect the endpoint
    asio::ip::tcp::endpoint endpoint(asio::ip::address::from_string(userver.m_addr), userver.m_port);
    m_socket.connect(endpoint);

    // ----------------------
    // validate the handshake
    // ----------------------
    exec::user::UserToBitHandshake u2b;
    u2b.set_channel(exec::shared::USER);
    u2b.set_rpc_version(1);
    u2b.set_support_listening(true);

    int coord_id = 1;
    OutBoundRpcMessage out_msg(exec::rpc::REQUEST, exec::user::HANDSHAKE, coord_id, &u2b);
    send_sync(out_msg);

    InBoundRpcMessage in_msg;
    recv_sync(in_msg);

    ctx.m_coord_id = in_msg.m_coord_id;
    if ( in_msg.m_mode == exec::rpc::RESPONSE ) {
        ctx.m_failure = false;
        // expect BitToUserHandshake
        exec::user::BitToUserHandshake b2u;
        b2u.ParseFromArray(in_msg.m_pbody.data(), in_msg.m_pbody.size());
        // check whether the rpc version is correct
        if (b2u.rpc_version() != u2b.rpc_version()) {
            cerr << "Invalid rpc version.  Expected << " <<u2b.rpc_version() << ", actual "<< b2u.rpc_version() << "." << endl;
        }
    } else if (in_msg.m_mode == exec::rpc::RESPONSE_FAILURE) {
        // TODO handle response failure
        ctx.m_failure = true;
        return;
    } else {
        cerr << "Unknown RPC mode!\n";
    }
}

void DrillClientSync2::ExecuteStatementDirect(const ExecutionContext& in_ctx,
        const exec::user::RunQuery& drill_query,
        ExecutionContext& ctx, RecordBatchBuffer& buffer) {
    cerr << "query = " << drill_query.plan() << endl;
    // send the query
    OutBoundRpcMessage out_msg(exec::rpc::REQUEST, exec::user::RUN_QUERY, in_ctx.m_coord_id + 1, &drill_query);
    send_sync(out_msg);

    // receive the query id
    InBoundRpcMessage in_msg;
    recv_sync(in_msg);
    if (in_msg.m_mode == exec::rpc::RESPONSE_FAILURE) {
        ctx.m_failure = true;
        return;
    } else if ( in_msg.m_mode == exec::rpc::RESPONSE) {
        ctx.m_failure = false;
        ctx.m_type = in_msg.m_rpc_type;
        cerr << "m_type = " << ctx.m_type << "\n";
        cerr << "QUERY HANLE = " << exec::user::QUERY_HANDLE << "\n";

        // expect query id
        exec::shared::QueryId qid;
#ifdef EXTRA_DEBUGGING
        cerr << "m_pbody = " << in_msg.m_pbody.size() << endl;
#endif

        qid.ParseFromArray(in_msg.m_pbody.data(), in_msg.m_pbody.size());

#ifdef EXTRA_DEBUGGING
        cerr << qid.DebugString() << endl;
#endif
    } else {
        cerr << "Unknown RPC mode!\n";
        return;
    }

    // get the result
    std::vector<InBoundRpcMessage> r_msgs(1024);
    exec::user::QueryResult result;
    int cnt = 0;
    do {

        cerr << "--> Receiving msg " << cnt + 1 << ": " << endl;
        recv_sync(in_msg);

        if (in_msg.m_mode == exec::rpc::RESPONSE_FAILURE) {
            ctx.m_failure = true;
            return;
        } else if (in_msg.m_mode == exec::rpc::RESPONSE) {
            ctx.m_failure = false;
            result.ParseFromArray(in_msg.m_pbody.data(), r_msgs[cnt].m_pbody.size());
#ifdef EXTRA_DEBUGGING
            cerr << result.DebugString() << endl;
#endif
            cnt ++;
        } else if (in_msg.m_mode == exec::rpc::REQUEST) {
            /// aha, we get a request...?
            result.ParseFromArray(in_msg.m_pbody.data(), in_msg.m_pbody.size());
#ifdef EXTRA_DEBUGGING
            cerr << "receive a REQUEST" << endl;
            cerr << result.DebugString() << endl;
#endif

        } else {
            cerr << "Unknown RPC mode!\n";
            return;
        }

    } while(!result.is_last_chunk());

    cerr << cnt + 1 << " messages received for results\n";

}

void DrillClientSync2::EndStatement(ExecutionContext& context) {
    // TODO: need coordination id
    // send ack
    exec::rpc::Ack ack;
    ack.set_ok(true);
    // coord_id = r_msgs[cnt-1].m_coord_id + 1;
    int coord_id = 10;
    OutBoundRpcMessage ack_msg(exec::rpc::RESPONSE, exec::user::ACK, coord_id, &ack);
    send_sync(ack_msg);
}

void DrillClientSync2::send_sync(OutBoundRpcMessage& msg) {
    m_encoder.Encode(m_wbuf, msg);
    m_socket.write_some(asio::buffer(m_wbuf));
}

void DrillClientSync2::recv_sync(InBoundRpcMessage& msg) {
    m_socket.read_some(asio::buffer(m_rbuf));
    uint32_t length = 0;
    int bytes_read = m_decoder.LengthDecode(m_rbuf.data(), &length);
    m_decoder.Decode(m_rbuf.data() + bytes_read, length, msg);
}

