#ifndef DRILL_CLIENT_H
#define DRILL_CLIENT_H
#include "common.h"
#include "rpc-encoder.h"
#include "rpc-decoder.h"
#include "rpc-message.h"


namespace Drill {

struct DrillConfig {
    int m_retry_time;
    int m_reconnect_times;
};

// a wrapper for the response
struct ExecutationContext {
    bool m_coord_id;                    // the coordination id for each request
    exec::user::QueryResult m_result;
    bool m_failure;                     // whether it is a request failure
    exec::rpc::RpcFailure m_failure;    // the failure message if we get failed
};

// Consider using asio::strand for async I/O
class DrillClient {

  public:
    virtual ~DrillClienit() = 0;
    
    /// connect to a zookeepr and ask for coordination 
    virtual void ZKConnect() = 0 ;
    /// get available drillbit
    virtual void ZKGetAvailableDrillbit(UserServerEndPoint& endpoint) = 0;
    /// close the connection with zookeeper
    virtual void ZKClose() = 0;


    /// @brief open a session to the user server endpoint coordinated by Drill Client
    /// 
    /// It will send a initial handshake request and validate the handshake.
    virtual bool OpenSession(ExecutationContext& context) = 0; 

    /// directly connect to a Drill User Server if we know the address (mainly for testing)
    virtual bool OpenSession(const UserServerEndPoint& endpoint, ExecutationContext& context) = 0;

    /// Test whether the client is active
    virtual bool Active() = 0;

    /// Reconnect if submission failed
    virtual bool Reconnect() = 0;

    /// Send Goodbye message to close the session
    void CloseSession();

    /// @brief Executes a SQL query in Apache Drill
    ///
    /// @param[in]  query    The query to execute. 
    /// @param[out] context  The output context generated from executing the query.
    /// @param[out] buffer   the output buffer
    virtual void ExecuteStatement(const string& query, ExecutationContext& context, RowSetBuffer& buffer) = 0;

    /// @brief Cancel the query
    virtual void CancelOpereation(ExecuteStatement& context) = 0;

    /// @brief close an operation by sending an Ack message to the server
    /// 
    /// @question What is the expected behaviour to send an Ack before the server send all results out?
    virtual void CloseOpereation(ExecuteStatement& context) = 0;
    

    /// @brief Fetch at most the specified number of rows for the result set.
    ///
    ///
    /// @param[in] context               The context of the query to fetch for.
    /// @param[out] buffer               The buffer to store the rows fetched.
    /// @param[in] maxRows               The max number of rows to fetch.
    void FetchNRows(
        ExecutationContext& in_context,
        RowSetBuffer& out_buffer,
        uint32_t max_rows);


    /// @brief Get the metadata of the result set 
    ///
    /// parse exec::shared::RecordBatchDef to get the result
    ///
    /// @param[in] context   The context of the query to get metadata for.
    /// @param[out] columns  The column holder to write to.
    void GetResultSetSchema(DiracExecutationContext& context, vector<string>& columns);

    //  ---------------------------------------------------------------------------------------------
    //  Use Drill information schema to get database metadata
    //
    //  - return results as vector of string
    //  - some field describe in Drill Information Schema is not used (added if needed in the future)
    //  ---------------------------------------------------------------------------------------------

    /// @brief Retreive the Catalogs of the database
    ///
    /// @param[out] the list of catalog available on this connection.
    void GetCatalogs(vector<string>&  catalogs);

    /// @brief Retreive the Schamata of the database
    ///
    /// @param[out] the list of schemata available on this connection.
    void GetSchemata(vector<string>& schemata);

    /// @brief Retrieve the list of tables.
    ///
    /// @param[in]  schema      The schema to show tables for. This must be a valid schema.
    /// @param[out] table_names  The vector to write the results to. Should be empty initially.
    void GetTableNames(const string& schema, vector<string>& table_names);

    /// @brief Retrieve the schema of a table.
    ///
    //  Query TABLES table in Drill Information Schema
    //
    /// @param[in]  catalog_name           The catalog name.
    /// @param[in]  schema_name            The schema.
    /// @param[in]  table_name             The table name.
    /// @param[out] select_columns        The vector for storing the fetched table schema.
    virtual void GetColumnNames(
        const string&  catalog_cheame,
        const string&  schema_name,
        const string&  table_name,
        vector<string>& select_columns);


}
};

} // namespace Drill


#endif
