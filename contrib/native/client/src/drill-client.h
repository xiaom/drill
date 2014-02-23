#ifndef DRILL_CLIENT_H
#define DRILL_CLIENT_H
#include "common.h"
#include "rpc-encoder.h"
#include "rpc-decoder.h"
#include "rpc-message.h"

// Notes:
//
// - vector::data() is in C++ 11 but works fine with Visual Studio 2010


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


namespace Drill {

class DrillClient {

  public:
    // TODO:
    //
    // DrillConfig:
    //      - reconncection_times (as in ExecConstants.BIT_RETRY_TIMES)
    //      - reconncection_delay (as in ExecConstants.BIT_RETRY_DELAY)
    //
    // Buffer Allocator

    // @brief  Constructor
    //
    // @param[in] io_service The io_service object create by asio

    DrillClient(asio::io_service& io_service):m_io_service(io_service),
        m_socket(io_service), m_rbuf(10240), m_wbuf(10240), m_rmsg_len(0) { };

    ~DrillClient() { };

    // connects the client to a Drillbit UserServer
    void Connect(const UserServerEndPoint& endpoint);

    // test whether the client is active
    bool Active();

    // reconncet if sumbission failed, retry is setting by m_reconnect_times
    bool Reconnect();
    void Close() ;
    /*
    void CloseAsync() {
        m_io_service.post(boost::bind(&DrillClient::Close, this));
    }
    */
    bool ValidateHandShake(); // throw expection if not valid

    QueryResultHandle SubmitQuery(exec::user::QueryType t, const string& plan);
    QueryResultHandle GetResult();
    void SubmitQuerySync(exec::user::QueryType t, const string& plan);
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

    void send_sync(OutBoundRpcMessage& msg);
    void recv_sync(InBoundRpcMessage& msg);

    
};

inline bool DrillClient::Active() {
    return true;
}
inline void DrillClient::Close() {
    m_socket.close();
}
inline bool DrillClient::Reconnect() {
    if (Active()) {
        return true;
    }
    /*
    int retry = m_reconnect_times;
    while( retry > 0 ){
        retry--;
        // ask the cluster coordinator for available drillbit

    }
    */
    return true;
}



} // namespace Drill

#endif
