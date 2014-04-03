#include <queue>
#include <vector>
#include <boost/asio.hpp>
#include <boost/thread.hpp>
#include <boost/bind.hpp>
#include <boost/log/trivial.hpp>

#include "drillClient.hpp"
#include "proto-cpp/UserBitShared.pb.h"
#include "drill-client-async.h"
//#include "common.h"
#include "rpc-encoder.h"
#include "rpc-decoder.h"
#include "rpc-message.h"
#include "recordBatch.h"

#ifdef DEBUG
#define BOOST_ASIO_ENABLE_HANDLER_TRACKING
#endif

using namespace std;
using namespace boost;
using namespace Drill;
using namespace exec::user;

void DrillClientImpl::Connect(const UserServerEndPoint& userver){
    // connect the endpoint
    //TODO: Handle connection failure
    if(!this->m_bIsConnected){
        boost::asio::ip::tcp::endpoint endpoint(boost::asio::ip::address::from_string(userver.m_addr), userver.m_port);
        m_socket.connect(endpoint);
    }
}

void DrillClientImpl::sendSync(OutBoundRpcMessage& msg){
    boost::lock_guard<boost::mutex> bufferLock(this->m_bufferMutex);
    m_encoder.Encode(m_wbuf, msg);
    m_socket.write_some(boost::asio::buffer(m_wbuf));
}

void DrillClientImpl::recvSync(InBoundRpcMessage& msg){
    boost::lock_guard<boost::mutex> bufferLock(this->m_bufferMutex);
    m_socket.read_some(boost::asio::buffer(m_rbuf));
    uint32_t length = 0;
    int bytes_read = m_decoder.LengthDecode(m_rbuf.data(), &length);
    m_decoder.Decode(m_rbuf.data() + bytes_read, length, msg);
}

bool DrillClientImpl::ValidateHandShake(){
    UserToBitHandshake u2b;
    u2b.set_channel(exec::shared::USER);
    u2b.set_rpc_version(1);
    u2b.set_support_listening(true);

    int coord_id = 1;

    OutBoundRpcMessage out_msg(exec::rpc::REQUEST, HANDSHAKE, coord_id, &u2b);
    sendSync(out_msg);

    InBoundRpcMessage in_msg;
    recvSync(in_msg);

    BitToUserHandshake b2u;
    b2u.ParseFromArray(in_msg.m_pbody.data(), in_msg.m_pbody.size());

    // validate handshake
    if (b2u.rpc_version() != u2b.rpc_version()) {
        BOOST_LOG_TRIVIAL(trace) << "Invalid rpc version.  Expected << " <<u2b.rpc_version() << ", actual "<< b2u.rpc_version() << "." ;
        return false;
    }
    return true;
}

DrillClientQueryResult* DrillClientImpl::SubmitQuery(QueryType t, const string& plan, pfnQueryResultsListener l){
    BOOST_LOG_TRIVIAL(trace) << "plan = " << plan;
    RunQuery query;
    query.set_results_mode(STREAM_FULL);
    query.set_type(t);
    query.set_plan(plan);

    //TODO: assign a new coordination id
    int coord_id = 133;
    OutBoundRpcMessage out_msg(exec::rpc::REQUEST, RUN_QUERY, coord_id, &query);
    sendSync(out_msg);

    BOOST_LOG_TRIVIAL(trace)  << "do read";
    InBoundRpcMessage in_msg;
    recvSync(in_msg);
    exec::shared::QueryId qid;
    BOOST_LOG_TRIVIAL(trace)  << "m_pbody = " << in_msg.m_pbody.size();
    qid.ParseFromArray(in_msg.m_pbody.data(), in_msg.m_pbody.size());
    BOOST_LOG_TRIVIAL(trace) << qid.DebugString();

    //TODO: Handle errors in query results.
    
    //Get Result
    DrillClientQueryResult* pQuery = new DrillClientQueryResult(this);
    this->m_queryResults.push_back(pQuery);
    pQuery->registerListener(l);
    pQuery->getResult();
    //run this in a new thread
    if(this->m_pListenerThread==NULL){
        this->m_pListenerThread= new boost::thread(boost::bind(&boost::asio::io_service::run, &this->m_io_service));
    }
    return pQuery;
}

void DrillClientImpl::waitForResults(){
        this->m_pListenerThread->join();
        this->m_pListenerThread=NULL;
}

void DrillClientQueryResult::getResult(){
    BOOST_LOG_TRIVIAL(trace)  << "Getting Results" << endl;
    getNextRecordBatch();
}

void DrillClientQueryResult::getNextRecordBatch() {
    BOOST_LOG_TRIVIAL(trace) << "Getting next record batch" << endl;
    //memset(m_readLengthBuf, 0, LEN_PREFIX_BUFLEN);
    async_read( 
        this->m_pClient->m_socket,
        boost::asio::buffer(m_readLengthBuf, LEN_PREFIX_BUFLEN),
        boost::bind(
            &DrillClientQueryResult::handleReadLength,
            this,
            boost::asio::placeholders::error, boost::asio::placeholders::bytes_transferred)
    );
}

void DrillClientQueryResult::handleReadLength(const boost::system::error_code & err, size_t bytes_transferred) {
    if (!err){
        BOOST_LOG_TRIVIAL(trace) << "> handle read length" << endl;
        int bytes_read = this->m_pClient->m_decoder.LengthDecode(this->m_readLengthBuf, &this->m_rmsgLen);
        BOOST_LOG_TRIVIAL(trace) << "bytes read = " << bytes_read << endl;
        BOOST_LOG_TRIVIAL(trace) << "m_rmsgLen = " << m_rmsgLen << endl;

        if(m_rmsgLen){
            boost::unique_lock<boost::mutex> bufferLock(m_bufferMutex);
            size_t leftover = LEN_PREFIX_BUFLEN - bytes_read;
            //bufferLock.lock();
            // Allocate a buffer
            BOOST_LOG_TRIVIAL(trace) << "Allocated and locked buffer." << endl;
            this->m_currentBuffer=allocateBuffer(m_rmsgLen);
            if(this->m_currentBuffer==NULL){
                //TODO: Throw out of memory exception
            }
            if(leftover){
                memcpy(m_currentBuffer, this->m_readLengthBuf + bytes_read, leftover);
            }
            this->m_dataBuffers.push_back(m_currentBuffer);
            async_read( this->m_pClient->m_socket,
                       boost::asio::buffer(m_currentBuffer + leftover, m_rmsgLen - leftover),
                        boost::bind(&DrillClientQueryResult::handleReadMsg, this,
                                    boost::asio::placeholders::error, boost::asio::placeholders::bytes_transferred)
                       );
        }
    }else{
        BOOST_LOG_TRIVIAL(trace) << "handle read length error: " << err << "\n";
    }
}

void DrillClientQueryResult::handleReadMsg(const boost::system::error_code & err, size_t bytes_transferred) {
    BOOST_LOG_TRIVIAL(trace) << "read msg" << endl;
    if (!err) {
        boost::unique_lock<boost::mutex> bufferLock(m_bufferMutex);
        BOOST_LOG_TRIVIAL(trace) << "Data Message: bytes read = " << bytes_transferred << endl;

        //bufferLock.lock();
        BOOST_LOG_TRIVIAL(trace) << "Locked buffer." << endl;
        InBoundRpcMessage msg;
        this->m_pClient->m_decoder.Decode(m_currentBuffer, m_rmsgLen, msg);
        BOOST_LOG_TRIVIAL(trace) << "Done decoding chunk" << endl;
        
        QueryResult* qr = new QueryResult; //Record Batch will own this object and free it up.

        if(msg.m_rpc_type==QUERY_RESULT){
            qr->ParseFromArray(msg.m_pbody.data(), msg.m_pbody.size());
            BOOST_LOG_TRIVIAL(trace) << qr->DebugString();
            //TODO: Check QueryResult.queryState. QueryResult could have an error.
            
            //Build Record Batch here 
            //RecordBatch* pRecordBatch= new RecordBatch(qr.def(), qr.row_count(), msg.m_dbody);
            setupColumnDefs(qr);
            RecordBatch* pRecordBatch= new RecordBatch(qr, msg.m_dbody);
            pRecordBatch->build();
            this->m_bIsQueryPending=true;
            this->m_bIsLastChunk=qr->is_last_chunk();
            status_t ret;
            if(this->m_pResultsListener!=NULL){
                ret = m_pResultsListener(this, pRecordBatch, NULL);
                //TODO: To free memory for the buffer requires the app to call 
                //destructor on the record batch pointer.
            }else{
                //Provide a default callback that is called when a record batch is received
                ret = this->defaultQueryResultsListener(this, pRecordBatch, NULL);
            }
            //TODO: check the return value and do not continue if the 
            //client app returns FAILURE
            
            if(qr->is_last_chunk()){
                BOOST_LOG_TRIVIAL(trace) << "Received last batch.";
                return;
            }
        }else{
            //TODO:If not QUERY_RESULT, then do appropriate error handling
            this->m_bIsQueryPending=false; // any blocked listener threads will move on
            BOOST_LOG_TRIVIAL(trace) << "QueryResult returned " << msg.m_rpc_type;
            return;
        }
        exec::rpc::Ack ack;
        ack.set_ok(true);
        OutBoundRpcMessage ack_msg(exec::rpc::RESPONSE, ACK, msg.m_coord_id, &ack);
        m_pClient->sendSync(ack_msg);
        BOOST_LOG_TRIVIAL(trace) << "ACK sent" << endl;
        getNextRecordBatch();
    }else{
        BOOST_LOG_TRIVIAL(trace) << "Error: " << err << "\n";
    }
    return;
}

void DrillClientQueryResult::setupColumnDefs(QueryResult* pQueryResult) {
    //TODO: this method needs to be thread safe along with getColumnDefs()
    bool hasSchemaChanged=false;
    if(this->m_columnDefs.empty()){
        //std::vector<const FieldMetadata*> fmds = this->m_columnDefs; // keep a copy
        //this->m_columnDefs.clear();
        size_t numFields=pQueryResult->def().field_size();
        for(size_t i=0; i<numFields; i++){
            const FieldMetadata* fmd=&pQueryResult->def().field(i);
            this->m_columnDefs.push_back(fmd);
        }
    }
    ////TODO:Look for changes in the vector and trigger a Schema change event if necessary
    return;
}

status_t DrillClientQueryResult::defaultQueryResultsListener(void* ctx, RecordBatch* b, DrillClientError* err) {
    //ctx; // unused, we already have the this pointer
    //TODO: check if the query has been canceled. IF so then return FAILURE. Caller will send cancel to the server.
    BOOST_LOG_TRIVIAL(trace) << "Query result listener called" << endl;
    if (!err) {
        // signal the cond var
        {
            BOOST_LOG_TRIVIAL(trace) << "Query result listener saved result to queue." << endl;
            boost::lock_guard<boost::mutex> bufferLock(this->m_cvMutex);
            this->m_recordBatches.push(b);
            this->m_bHasData=true;
        }
        m_cv.notify_one();
    }else{
        //TODO: BOOST_LOG_TRIVIAL(trace) << "Error: " << err->msg() << "\n";
        return QRY_FAILURE;
    }
    return QRY_SUCCESS;
}

RecordBatch*  DrillClientQueryResult::getNext() {
    RecordBatch* pRecordBatch=NULL;
    //if no more data, return NULL;
    if(!m_bIsQueryPending) return NULL;

    boost::unique_lock<boost::mutex> bufferLock(this->m_cvMutex);
    BOOST_LOG_TRIVIAL(trace) << "Synchronous read waiting for data." << endl;
    while(!this->m_bHasData) {
        this->m_cv.wait(bufferLock);
    }
    // remove first element from queue
    pRecordBatch = this->m_recordBatches.front();
    this->m_recordBatches.pop();
    this->m_bHasData=!this->m_recordBatches.empty();
    // if vector is empty, set m_bHasDataPending to false;
    m_bIsQueryPending=!(this->m_recordBatches.empty()&&m_bIsLastChunk);
    return pRecordBatch;
}

// Blocks until data is available
void DrillClientQueryResult::waitForData() {
    //if no more data, return NULL;
    if(!m_bIsQueryPending) return;
    boost::unique_lock<boost::mutex> bufferLock(this->m_cvMutex);
    while(!this->m_bHasData) {
        this->m_cv.wait(bufferLock);
    }
}

