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
//#include "drill-async.hpp"
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
    b->print(b->getNumRecords() < 10? b->getNumRecords() : 10); // print at most 10 records
    return QRY_SUCCESS ;
}

int main(int argc, char* argv[]) {
    try {

        //string drill_addr = "192.168.202.156";
        //string drill_addr = "10.0.28.124";
        string drill_addr = "127.0.0.1";
        int port=31010;
		UserServerEndPoint user_server(drill_addr,port);
		// use async public api
        // string queryType="sync";
        // string apiType="usepublicapi";
        
		std::vector<std::string> plans(1); 
		//plans[0] = "select * from `INFORMATION_SCHEMA`.`SCHEMATA`";
		plans[0] = "select * from dfs.`/opt/drill/data/json/test.json`";
		//plans[0] = "select * from `hivestg`.`integer_table`";
		//plans[1] = "select * from INFORMATION_SCHEMA.`TABLES`";

            
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

	std::cout << "\nContinue...\n";
	char placeholder;
	std::cin >> placeholder;
    return 0;
}

