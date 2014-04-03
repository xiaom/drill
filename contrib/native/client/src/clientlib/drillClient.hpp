#ifndef DRILL_CLIENT_H
#define DRILL_CLIENT_H

#include <vector>
#include "common.h"
#include "../proto-cpp/User.pb.h"

using namespace std;
using namespace exec::shared;

#ifdef WIN32
#ifdef DRILL_CLIENT_EXPORTS
#define DECLSPEC_DRILL_CLIENT __declspec(dllexport)
#else
#define DECLSPEC_DRILL_CLIENT __declspec(dllimport)
#endif
#else
#define DECLSPEC_DRILL_CLIENT
#endif

namespace Drill {

    struct UserServerEndPoint;
    class  DrillClientImpl;
    class  DrillClientQueryResult;
    class  FieldDefinition;
    class  RecordBatch;
    class  SchemaDef;

    class DECLSPEC_DRILL_CLIENT DrillClientError{
        public:
            DrillClientError(){errno=0; msg=NULL;};
            DrillClientError(int e, char* m){errno=e; msg=m;};
            int errno;
            char* msg;
    };

    //typedef bool status_t;
    typedef enum{QRY_SUCCESS=0, QRY_FAILURE=1, QRY_SUCCESS_WITH_INFO=2, QRY_NO_MORE_DATA=3, QRY_OUT_OF_BOUNDS=4} status_t;
    typedef enum{CONN_SUCCESS=0, CONN_FAILURE=1, CONN_HANDSHAKE_FAILED=2} connectionStatus_t;

    /*
     * Query Results listener callback. This function is called for every record batch after it has 
     * been received and decoded. The listener function should return a status. 
     * If the listener returns failure, the query will be canceled.
     */
    typedef status_t (*pfnQueryResultsListener)(void* ctx, RecordBatch* b, DrillClientError* err);
    /*
     * The schema change listener callback. This function is called if the record batch detects a
     * change in the schema. The client application can call getColDefs in the RecordIterator or 
     * get the field information from the RecordBatch itself and handle the change appropriately.
     */
    typedef uint32_t (*pfnSchemaListener)(void* ctx, SchemaDef* s, DrillClientError* err);

    /* 
     * A Record Iterator instance is returned by the SubmitQuery class. Calls block until some data 
     * is available, or until all data has been returned.
     */

    class DECLSPEC_DRILL_CLIENT RecordIterator{
        friend class DrillClient;
        public:
            
            /* Returns a vector of column(i.e. field) definitions */
            vector<const FieldMetadata*>& getColDefs();
            
            /* Move the current pointer to the next record. */
            status_t next();
            
            /* Gets the ith column in the current record. */
            status_t getCol(size_t i, void** b, size_t* sz);
            
            /* Cancels the query. */
            status_t cancel();

            //void waitForResults();
            void registerSchemaChangeListener(pfnSchemaListener* l);

        private:
            RecordIterator(DrillClientQueryResult* pResult){
                this->m_currentRecord=-1;
                this->m_pCurrentRecordBatch=NULL;
                this->m_pQueryResult=pResult;
            }
            DrillClientQueryResult* m_pQueryResult;
            size_t m_currentRecord;
            RecordBatch* m_pCurrentRecordBatch;
    };

    class DECLSPEC_DRILL_CLIENT DrillClient{
        public:
            DrillClient();
            ~DrillClient();

            /* connects the client to a Drillbit UserServer. */
            connectionStatus_t connect(const UserServerEndPoint& endpoint);

            /* test whether the client is active */
            bool isActive();

            /*  close the connection. cancel any pending requests. */
            void close() ;

            /*Submit a query asynchronously and wait for results to be returned thru a callback */
            status_t submitQuery(exec::user::QueryType t, const string& plan, pfnQueryResultsListener listener);

            /*
             * Submit a query asynchronously and wait for results to be returned thru an iterator that returns
             * results synchronously 
             */
            RecordIterator* submitQuery(exec::user::QueryType t, const string& plan, DrillClientError* err);

            /* 
             * The client application should call this function to wait for results if it has registered a 
             * listener.
             */
            void waitForResults();

        private:

            DrillClientImpl * m_pImpl;

    };

} // namespace Drill

#endif
