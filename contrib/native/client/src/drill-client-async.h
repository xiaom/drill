#ifndef DRILL_CLIENT_ASYNC_H
#define DRILL_CLIENT_ASYNC_H

#include <vector>
#include <queue>
#include <boost/asio.hpp>
#include <boost/thread.hpp>

#include "proto-cpp/User.pb.h"
#include "common.h"
#include "rpc-encoder.h"
#include "rpc-decoder.h"
#include "drillClient.hpp"

using namespace std;
using namespace boost;
using namespace exec::shared;
using namespace exec::user;
namespace asio = boost::asio;

namespace Drill {

    class vector;
    class queue;
    class DrillClientImpl;
    class InBoundRpcMessage;
    class OutBoundRpcMessage;
    class RecordBatch;
    class RpcEncoder;
    class RpcDecoder;
    struct UserServerEndPoint;

    class DrillClientQueryResult{
        public:
            DrillClientQueryResult(DrillClientImpl * pClient):
                m_pClient(pClient),
                m_rmsgLen(0),
                m_currentBuffer(NULL),
                m_bHasData(false),
                m_bIsQueryPending(true),
                m_bIsLastChunk(false),
                m_pResultsListener(NULL)
            {};

            ~DrillClientQueryResult(){
                this->clearAndDestroy();
            };

            void getResult();

            // get data asynchronously
            void registerListener(pfnQueryResultsListener listener){
                this->m_pResultsListener=listener;
            }

            // Synchronous call to get data
            RecordBatch*  getNext();
            // Blocks until data is available.
            void waitForData();

            std::vector<const FieldMetadata*>& getColumnDefs(){ return this->m_columnDefs;}

        private:

            DrillClientImpl* m_pClient;

            // Vector of Buffers holding data returned by the server
            // Each data buffer is decoded into a RecordBatch
            std::vector<ByteBuf_t> m_dataBuffers;
            std::queue<RecordBatch*> m_recordBatches;
            std::vector<const FieldMetadata*> m_columnDefs;

            //length ofthe current data msg being received
            uint32_t m_rmsgLen;
            //Pointer to the current data buffer being received and processed
            ByteBuf_t m_currentBuffer;
            Byte_t m_readLengthBuf[LEN_PREFIX_BUFLEN];

            // Mutex to protect read buffer. Is this needed???
            boost::mutex m_bufferMutex; 
            // Mutex for Cond variable for read write to batch vector
            boost::mutex m_cvMutex; 
            // Condition variable to signal arrival of more data. Condition variable is signaled 
            // if the recordBatches queue is not empty
            boost::condition_variable m_cv; 
            bool m_bHasData;
            
            // if m_bIsQueryPending is true, we continue to wait for results
            bool m_bIsQueryPending;

            bool m_bIsLastChunk;

            // Results callback
            pfnQueryResultsListener m_pResultsListener;

            ByteBuf_t allocateBuffer(size_t len){
                ByteBuf_t b = (ByteBuf_t)malloc(len);
                memset(b, 0, len);
                return b;
            }

            void freeBuffer(ByteBuf_t b){
                free(b);
            }

            void getNextRecordBatch();
            // get the length of the data being sent and start an async read to read that data
            void handleReadLength(const boost::system::error_code & err, size_t bytes_transferred) ;
            void handleReadMsg(const boost::system::error_code & err, size_t bytes_transferred) ;
            void setupColumnDefs(QueryResult* pQueryResult);


            status_t defaultQueryResultsListener(void* ctx, RecordBatch* b, DrillClientError* err);

            void clearAndDestroy(){
                //TODO: Free any record batches or buffers remaining
                //TODO: Cancel any pending requests
            }
    };

    class DrillClientImpl {
        friend class DrillClientQueryResult;

        public:
        DrillClientImpl():m_socket(m_io_service), m_pListenerThread(NULL),
        m_rbuf(1024), m_wbuf(1024)  {
            m_bIsConnected=false;
        };

        ~DrillClientImpl(){ 
            //TODO: Free any record batches or buffers remaining
            //TODO: Cancel any pending requests
            //TODO: Clear and destroy DrillClientQueryResults vector
            //Terminate and free the listener thread
            if(this->m_pListenerThread!=NULL){
                this->m_pListenerThread->interrupt();
                delete this->m_pListenerThread;
            }
        };

        // connects the client to a Drillbit UserServer
        void Connect(const UserServerEndPoint& endpoint);

        // test whether the client is active
        bool Active();

        // reconnet if sumbission failed, retry is setting by m_reconnect_times
        bool Reconnect();
        void Close() ;
        bool ValidateHandShake(); // throw expection if not valid

        DrillClientQueryResult* SubmitQuery(exec::user::QueryType t, const string& plan, pfnQueryResultsListener listener);
        void waitForResults();

        private:

        bool m_bIsConnected;
        boost::asio::io_service m_io_service;
        boost::asio::ip::tcp::socket m_socket;
        boost::thread * m_pListenerThread;

        //for synchronous messages, like validate handshake
        DataBuf m_rbuf; // buffer for receiving synchronous messages
        DataBuf m_wbuf; // buffer for sending synchronous message
        // Mutex to protect read/write buffer.
        boost::mutex m_bufferMutex; 

        // Send and receive synchronous messages
        void sendSync(OutBoundRpcMessage& msg);
        void recvSync(InBoundRpcMessage& msg);

        RpcEncoder m_encoder;
        RpcDecoder m_decoder;

        std::vector<DrillClientQueryResult*> m_queryResults;

    };

    inline bool DrillClientImpl::Active() {
        return this->m_bIsConnected;;
    }

    inline void DrillClientImpl::Close() {
        //TODO: cancel pending query
        if(this->m_bIsConnected){
            m_socket.close();
            m_bIsConnected=false;
        }
    }

    inline bool DrillClientImpl::Reconnect() {
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
