#ifndef DRILL_CLIENT_H
#define DRILL_CLIENT_H
#include "common.h"

namespace Drill {

struct DrillConfig {
    int m_retry_time;
    int m_reconnect_times;
};

// Wrapper for response of each request
struct ExecutionContext {
    bool m_coord_id;                    // the coordination id for each request
	exec::user::QueryResult m_result;   // including query_id, record count and so on 
    bool m_failure;                     // whether it is a request failure
    exec::rpc::RpcFailure m_failure;    // the failure message if we get failed
};

class ClusterCoordinator {
	/// @brief connect to a quorum of zookeeper 
	virtual void Connect() = 0 ;
	/// @brief refresh the cluster 
	virtual void Refresh() = 0 ;
	/// @brief get an available drillbit
	virtual void GetQueryCoordinator(UserServerEndPoint& endpoint) = 0;
	/// @brief close the connection to the quorum
	virtual void Close() = 0;
};

class RecordBatchBuffer{
	vector<RecordBatch> m_buffer;
	
	/// Provide functions to iterate recordbatch, convert to list of strings and so on
};


class DrillClient {

  public:
  
	virtual ~DrillClient() = 0;
    
    /// @brief Open a session to the user server endpoint coordinated by quorum
    /// 
    /// It will send an initial handshake request and validate the handshake.
	/// The reconnection should also been done by 
	/// 
	/// @param[in]  quorum
	/// @param[out] context
    virtual void OpenSession(const ClusterCoordinator& quorum, ExecutionContext& context) = 0; 
	

    /// @brief Directly connect to a Drill user server if we know the address
	///
	/// It will send an initial handshake request and validate the handshake.
	///
	/// @param[in]  quorum
	/// @param[out] context
    virtual void OpenSession(const UserServerEndPoint& endpoint, ExecutionContext& context) = 0;

    /// @brief Send Goodbye message to close the session
    virtual void CloseSession(ExecutionContext& context);

    /// @brief Prepares a SQL query and retrieve result-set meta-data from a query
	/// 
	/// We anticipate two classes of queries: meta-data enquiries and bona-fide queries.
    ///
    /// @param[in]  query    The query to execute. 
    /// @param[out] context  The output context generated from preparing the query.
    /// @param[out] buffer   The output buffer
    virtual void PrepareStatement(const string& query, ExecutionContext& context, RecordBatchBuffer& buffer) = 0;

    /// @brief Bind parameters values for a previously prepared SQL query 
    ///
    /// @param[in] context  	The context of a previously prepared query. Use query id to track the prepared query statement 
    /// @param[in] parameter  	The list of (key,value) pairs of parameters. 
	///							For example, could be a format like "key1:val1, key2:val2 "
	/// @param[out] context 	The output context generated from binding statement
    virtual void BindStatement(const ExecutionContext& in_context, const string& parameters, ExecutionContext& out_context) = 0;

    /// @brief Executes a SQL query in Apache Drill. 
	/// 
	/// We anticipate two classes of queries: meta-data enquiries and bona-fide queries.
    ///
    /// @param[in]  in_context  The context of a previously prepared/bind query.
	/// @param[out] out_context The output context generated from executing the query
	/// @param[out] buffer   	The output buffer.
    virtual void ExecuteStatement(const ExecutionContext& in_context, ExecutionContext& out_context, RecordBatchBuffer& buffer) = 0;

    /// @brief Cancel the query
	///
    virtual void CancelStatement(ExecutionContext& context) = 0;

    /// @brief Clean up a statement at the conclusion by sending an Ack message to the server
    /// 
    /// @question What is the expected behaviour to send an Ack before the server send all results out?
    virtual void EndStatement(ExecutionContext& context) = 0;
}
};

} // namespace Drill


#endif
