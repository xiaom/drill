#ifndef DRILL_CLIENT_ASYNC_H
#define DRILL_CLIENT_ASYNC_H
#include "common.h"
#include "recordBatch.h"
#include "rpc-encoder.h"
#include "rpc-decoder.h"
#include "rpc-message.h"

namespace Drill {

class DrillClientAsync {
  public:
    DrillClientAsync(asio::io_service& io_service):/*m_io_service(io_service),*/
        m_socket(io_service), m_rbuf(10240), m_wbuf(10240), m_rmsg_len(0), m_currentBuffer(0)/*, m_currentBufferSize(0)*/{ };

    ~DrillClientAsync() { 
        //TODO: Free any record batches or buffers remaining
    };

    // connects the client to a Drillbit UserServer
    void Connect(const UserServerEndPoint& endpoint);

    // test whether the client is active
    bool Active();

    // reconnet if sumbission failed, retry is setting by m_reconnect_times
    bool Reconnect();
    void Close() ;
    bool ValidateHandShake(); // throw expection if not valid

    void SubmitQuery(exec::user::QueryType t, const string& plan);
    void GetResult();
    //TODO: RegisterListener();

  private:
    void GetNextRecordBatch();
    //void do_read();
    // get the lenght of the data being sent and start an async read to read that data
    void handle_read_length(const boost::system::error_code & err, size_t bytes_transferred) ;
    void handle_read_msg(const boost::system::error_code & err, size_t bytes_transferred) ;
    // future<QueryResult> f_results;
    //asio::io_service& m_io_service;
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


    // Vector of Buffers holding data returned by the server
    // Each data buffer is decoded into a RecordBatch
    std::vector<ByteBuf_t> m_dataBuffers;
    std::vector<RecordBatch*> m_recordBatches;

    // //Pointer to the current data buffer being received and processed
    ByteBuf_t m_currentBuffer;
    //size_t m_currentBufferSize;
    Byte_t m_readLengthBuf[LEN_PREFIX_BUFLEN];
     

	ByteBuf_t allocateBuffer(size_t len){
			ByteBuf_t b = (ByteBuf_t)malloc(len);
			memset(b, 0, len);
			return b;
	}

	void freeBuffer(ByteBuf_t b){
		free(b);
	}


};

inline bool DrillClientAsync::Active() {
    return true;
}
inline void DrillClientAsync::Close() {
    //TODO: cancel pending query
    m_socket.close();
}
inline bool DrillClientAsync::Reconnect() {
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
