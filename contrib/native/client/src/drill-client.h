#ifndef DRILL_CLIENT_H
#define DRILL_CLIENT_H
#include "common.h"
#include "rpc-message.h"
#include "rpc-encoder.h"
#include "rpc-decoder.h"

namespace Drill {

struct DrillConfig {
    int m_retry_time;
    int m_reconnect_times;
};

// Wrapper of response for each request
struct ExecutionContext {
    int m_coord_id;  // the coordination id for each request
    bool m_failure;  // true if RpcMode is RESPONSE_FAILURE
    int m_type;      // type of the response, could be QUERY_RESULT or QUERY_HANDLE
    google::protobuf::MessageLite*  m_response;  // the response from the server.
    // one of { exec::user::QueryResult, exec::user::exec::rpc::RpcFailure
};

class ClusterCoordinator {
    /// @brief connect to a quorum of zookeeper
    virtual void Connect() ;
    /// @brief refresh the cluster
    virtual void Refresh() ;
    /// @brief get an available drillbit
    virtual void GetQueryCoordinator(UserServerEndPoint& endpoint);
    /// @brief close the connection to the quorum
    virtual void Close();
};

typedef string RecordBatch;
class RecordBatchBuffer {
    std::vector<RecordBatch> m_buffer;

    /// Provide functions to iterate recordbatch, convert to list of strings and so on
};

// The interface for DrillClient
//
// - change to pure virtual function after stablizing the API
class DrillClient {

  public:

    virtual ~DrillClient() {

    }

    /// @brief Open a session to the user server endpoint coordinated by quorum
    ///
    /// It will send an initial handshake request and validate the handshake.
    /// The reconnection should also been done by
    ///
    /// @param[in]  quorum
    /// @param[out] context
    virtual void OpenSession(const ClusterCoordinator& quorum, ExecutionContext& context) {

    }


    /// @brief Directly connect to a Drill user server if we know the address
    ///
    /// It will send an initial handshake request and validate the handshake.
    ///
    /// @param[in]  quorum
    /// @param[out] context
    virtual void OpenSession(const UserServerEndPoint& endpoint, ExecutionContext& context) = 0;

    /// @brief Send Goodbye message to close the session
    virtual void CloseSession(ExecutionContext& context) {

    }

    /// @brief Prepares a SQL query and retrieve result-set meta-data from a query
    ///
    /// We anticipate two classes of queries: meta-data enquiries and bona-fide queries.
    ///
    /// @param[in]  query    The query to execute.
    /// @param[out] context  The output context generated from preparing the query.
    /// @param[out] buffer   The output buffer
    virtual void PrepareStatement(
        const string& query,
        ExecutionContext& context,
        RecordBatchBuffer& buffer) {

    }

    /// @brief Bind parameters values for a previously prepared SQL query
    ///
    /// @param[in] context  	The context of a previously prepared query. Use query id to track the prepared query statement
    /// @param[in] parameter  	The list of (key,value) pairs of parameters.
    ///							For example, could be a format like "key1:val1, key2:val2 "
    /// @param[out] context 	The output context generated from binding statement
    virtual void BindStatement(
        const ExecutionContext& in_context,
        const string& parameters,
        ExecutionContext& out_context) {

    }

    /// @brief Executes a SQL query in Apache Drill.
    ///
    /// We anticipate two classes of queries: meta-data enquiries and bona-fide queries.
    ///
    /// @param[in]  in_context  The context of a previously prepared/bind query.
    /// @param[out] out_context The output context generated from executing the query
    /// @param[out] buffer   	The output buffer.
    virtual void ExecuteStatement(
        const ExecutionContext& in_context,
        ExecutionContext& out_context,
        RecordBatchBuffer& buffer) {

    }

    /// @brief Execute a query directly
    ///
    /// add options to submit physical/logical query plan
    virtual void ExecuteStatementDirect(
        const ExecutionContext& in_ctx,
        const string& query,
        ExecutionContext& context,
        RecordBatchBuffer& buffer) {
    }

    /// @brief Cancel the query
    ///
    virtual void CancelStatement(ExecutionContext& context) {

    }

    /// @brief Clean up a statement at the conclusion by sending an Ack message to the server
    ///
    /// @question What is the expected behaviour to send an Ack before the server send all results out?
    virtual void EndStatement(ExecutionContext& context) {

    }
};


class DrillClientSync: DrillClient {
  public:
    DrillClientSync(asio::io_service& io_service):
        m_io_service(io_service), m_socket(io_service),
        m_rbuf(10240), m_wbuf(10240), m_rmsg_len(0) { }

    ~DrillClientSync() { };

    void OpenSession(const UserServerEndPoint& endpoint, ExecutionContext& context);
    void CloseSession(ExecutionContext& context) {

    }

    void PrepareStatement(
        const string& query,
        ExecutionContext& context,
        RecordBatchBuffer& buffer) {

    }

    void BindStatement(
        const ExecutionContext& in_context,
        const string& parameters,
        ExecutionContext& out_context) {

    }

    void ExecuteStatement(
        const ExecutionContext& in_context,
        ExecutionContext& out_context,
        RecordBatchBuffer& buffer) {

    }

    void ExecuteStatementDirect(const ExecutionContext& in_ctx, const exec::user::RunQuery & drill_query,
                                ExecutionContext& ctx, RecordBatchBuffer& buffer);
    void CancelStatement(ExecutionContext& context) {

    }
    void EndStatement(ExecutionContext& context);
  private:
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
} // namespace Drill


#endif
