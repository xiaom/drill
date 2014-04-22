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
#include "clientlib/drillClient.hpp"
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
                case DM_REQUIRED:
                    sprintf((char*)printBuffer, "%lld", *(uint64_t*)buf);
                case DM_OPTIONAL:
                    break;
                case DM_REPEATED:
                    break;
            }
            break;
        case VARBINARY:
            switch (mode) {
                case DM_REQUIRED:
                    memcpy(printBuffer, buf, sz);
                case DM_OPTIONAL:
                    break;
                case DM_REPEATED:
                    break;
            }
            break;
        case VARCHAR:
            switch (mode) {
                case DM_REQUIRED:
                    memcpy(printBuffer, buf, sz);
                case DM_OPTIONAL:
                    break;
                case DM_REPEATED:
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

		
        //string drill_addr = "10.0.28.124";
        string drill_addr = "127.0.0.1";
        int port=31010;
		UserServerEndPoint user_server(drill_addr,port);
		// use async public api
        // string queryType="sync";
        // string apiType="usepublicapi";
        
		std::vector<std::string> plans(2); 
		plans[0] = "select * from INFORMATION_SCHEMA.SCHEMATA";
		plans[1] = "select * from INFORMATION_SCHEMA.`TABLES`";


        DrillClient client;
        client.connect(user_server);

		for(int i = 0; i< plans.size() ; i++){
			std::string& plan = plans[i];
			QueryHandle_t qryHandle=NULL;
			client.submitQuery(exec::user::SQL, plan, QueryResultsListener, NULL, &qryHandle);
			client.waitForResults();
			client.freeQueryResources(&qryHandle);
		}
		client.close();
        
    } catch (std::exception& e) {
        cerr << e.what() << endl;
    }

	std::cout << "\nContinue?\n";
	char placeholder;
	std::cin >> placeholder;
    return 0;
}

