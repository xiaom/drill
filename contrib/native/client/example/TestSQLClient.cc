//#pragma warning (disable: 4996)

//#include <cassert>
#include <stdio.h>
//#include <iostream>
#include <fstream>
//#include <cstring>

//#include <boost/cstdint.hpp>
#include <boost/asio.hpp>
//#include <boost/bind.hpp>
//#include <boost/thread.hpp>
//#include <boost/log/trivial.hpp>
//#include <google/protobuf/message_lite.h>
//#include <google/protobuf/wire_format_lite.h>

//#include "proto-cpp/GeneralRPC.pb.h"
//#include "proto-cpp/SchemaDef.pb.h"
//#include "proto-cpp/UserBitShared.pb.h"
//#include "proto-cpp/User.pb.h"
//#include "proto-cpp/ExecutionProtos.pb.h"




#include "common.h"
#include "drill-client-async.h"
#include "recordBatch.h"
#include "proto-cpp/Types.pb.h"
#include "proto-cpp/User.pb.h"

//#include "rpc-message.h"
//using exec::rpc::RpcMode;


//using std::cout;
//using std::cerr;
//using std::endl;

//using std::ifstream;
//using std::ostream;
//using std::memmove;
//using exec::rpc::RpcMode;

//namespace asio = boost::asio;
//
using namespace exec;
using namespace common;



using namespace Drill;

status_t QueryResultsListener(DrillClientQueryResult* drillQueryResult, RecordBatch* b, DrillClientError* err){
    b->print(10); // print at most 10 rows per batch
    return QRY_SUCCESS ;
}

void print(const FieldMetadata* pFieldMetadata, void* buf, size_t sz){
    const FieldDef& fieldDef = pFieldMetadata->def();
    const MajorType& majorType=fieldDef.major_type();
    int type = majorType.minor_type();
    int mode = majorType.mode();
    unsigned char printBuffer[10240];
    memset(printBuffer, 0, sizeof(printBuffer));
    switch (type) {
        case BIGINT:
            switch (mode) {
                case REQUIRED:
                    sprintf((char*)printBuffer, "%lld", *(uint64_t*)buf);
                case OPTIONAL:
                    break;
                case REPEATED:
                    break;
            }
            break;
        case VARBINARY:
            switch (mode) {
                case REQUIRED:
                    memcpy(printBuffer, buf, sz);
                case OPTIONAL:
                    break;
                case REPEATED:
                    break;
            }
            break;
        case VARCHAR:
            switch (mode) {
                case REQUIRED:
                    memcpy(printBuffer, buf, sz);
                case OPTIONAL:
                    break;
                case REPEATED:
                    break;
            }
            break;
        default:
            //memcpy(printBuffer, buf, sz);
            sprintf((char*)printBuffer, "NIY");
            break;
    }
    printf("%s\t", (char*)printBuffer);
    return;
}

int main(int argc, char* argv[]) {
    try {

        string drill_addr = "127.0.0.1";
        int port=31010;
        string queryType="sync";
        string apiType="usepublicapi";
        string plan = "select * from INFORMATION_SCHEMA.SCHEMATA";

        UserServerEndPoint user_server(drill_addr,port);
        if(apiType=="useinternalapi"){
            DrillClientImpl client;
            client.Connect(user_server);
            cerr << "Connected!\n" << endl;
            cerr << "Handshaking..." << endl;
            if (client.ValidateHandShake())
                cerr << "Handshake Succeeded!\n" << endl;
            cerr << "plan = " << plan << endl;

            if(queryType=="sync"){
                DrillClientQueryResult* pClientQuery = client.SubmitQuery(exec::user::SQL, plan, NULL, NULL);
                RecordBatch* pR=NULL;
                while((pR=pClientQuery->getNext())!=NULL){
                    pR->print(2);
                    delete pR;
                }
            }else{
                DrillClientQueryResult* pClientQuery = client.SubmitQuery(exec::user::SQL, plan, QueryResultsListener, NULL);
                client.waitForResults();
            }
            client.Close();
        }else{
            DrillClient client;
            client.connect(user_server);
            cerr << "Connected!\n" << endl;
            cerr << "plan = " << plan << endl;

            if(queryType=="sync"){
                DrillClientError* err=NULL;
                status_t ret;
                RecordIterator* pIter = client.submitQuery(exec::user::SQL, plan, err);
                size_t row=0;
                if(pIter){
                    // get fields.
                    std::vector<FieldMetadata*> fields = pIter->getColDefs();
                    while((ret=pIter->next())==QRY_SUCCESS){
                        row++;
                        if(row%4095==0){
                            for(size_t i=0; i<fields.size(); i++){
                                std::string name= fields[i]->def().name(0).name();
                                printf("%s\t", name.c_str());
                            }
                        }
                        printf("ROW: %ld\t", row);
                        for(size_t i=0; i<fields.size(); i++){
                            void* pBuf; size_t sz;
                            pIter->getCol(i, &pBuf, &sz);
                            print(fields[i], pBuf, sz);
                        }
                        printf("\n");
                    }
                }
                //delete pIter;
                client.freeQueryIterator(&pIter);
            }else{
                QueryHandle_t qryHandle=NULL;
                client.submitQuery(exec::user::PHYSICAL, plan, QueryResultsListener, NULL, &qryHandle);
                client.waitForResults();
                client.freeQueryResources(&qryHandle);
            }
            client.close();
        }
    } catch (std::exception& e) {
        cerr << e.what() << endl;
    }

    return 0;
}

