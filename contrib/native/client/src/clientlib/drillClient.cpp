#include <boost/log/trivial.hpp>
#include "drillClient.hpp"
#include "../recordBatch.h"
#include "../drill-client-async.h"

using namespace Drill;


vector<const FieldMetadata*>&  RecordIterator::getColDefs(){
    this->m_pQueryResult->waitForData();
    return this->m_pQueryResult->getColumnDefs();
}

status_t RecordIterator::next(){
    status_t ret=QRY_SUCCESS;
    this->m_pQueryResult->waitForData();
    m_currentRecord++;
    if(this->m_pCurrentRecordBatch==NULL || m_currentRecord==this->m_pCurrentRecordBatch->getNumRecords()){
        //TODO: delete this->m_pCurrentRecordBatch; // free the previous record batch
        this->m_currentRecord=0;
        this->m_pCurrentRecordBatch=this->m_pQueryResult->getNext();
        BOOST_LOG_TRIVIAL(trace) << "Fetched new Record batch " ;
        if(this->m_pCurrentRecordBatch==NULL || this->m_pCurrentRecordBatch->getNumRecords()==0){
            BOOST_LOG_TRIVIAL(trace) << "No more data." ;
            ret = QRY_NO_MORE_DATA;
        }
    }
    return ret;
}

/* Gets the ith column in the current record. */
status_t RecordIterator::getCol(size_t i, void** b, size_t* sz){
    //check fields out of bounds
    if(i>=getColDefs().size()) return QRY_OUT_OF_BOUNDS;
    //return raw byte buffer
    *b=this->m_pCurrentRecordBatch->getFields()[i]->getVector()->getRaw(this->m_currentRecord);
    *sz=this->m_pCurrentRecordBatch->getFields()[i]->getVector()->getSize(this->m_currentRecord);
    return QRY_SUCCESS;
}

status_t RecordIterator::cancel(){
    //TODO:
}

void RecordIterator::registerSchemaChangeListener(pfnSchemaListener* l){
    //TODO:
}

DrillClient::DrillClient(){
    this->m_pImpl=new DrillClientImpl;
}

DrillClient::~DrillClient(){
    delete this->m_pImpl;
}

connectionStatus_t DrillClient::connect(const UserServerEndPoint& endpoint){
    connectionStatus_t ret=CONN_SUCCESS;
    //TODO: handle connection failure
    this->m_pImpl->Connect(endpoint);
    
    //TODO: handle Handshake failure
    ret=this->m_pImpl->ValidateHandShake()?CONN_SUCCESS:CONN_HANDSHAKE_FAILED;
    return ret;

}

bool DrillClient::isActive(){
    return this->m_pImpl->Active();
}

void DrillClient::close() {
    this->m_pImpl->Close();
}

status_t DrillClient::submitQuery(exec::user::QueryType t, const string& plan, pfnQueryResultsListener listener){
    //TODO: Handle query execution errors
    this->m_pImpl->SubmitQuery(t, plan, listener);
    return QRY_SUCCESS; 
}

RecordIterator* DrillClient::submitQuery(exec::user::QueryType t, const string& plan, DrillClientError* err){
    //TODO: Handle query execution errors
    RecordIterator* pIter=NULL;
    DrillClientQueryResult* pResult=this->m_pImpl->SubmitQuery(t, plan, NULL);
    if(pResult){
        pIter=new RecordIterator(pResult);
    }
    return pIter;
}

void DrillClient::waitForResults(){
    this->m_pImpl->waitForResults();
}

