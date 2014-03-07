#include "common.h"
#include "drill-client.h"
#include "rpc-message.h"
using namespace Drill;

int main(int argc, char* argv[]) {
    try {

        string drill_addr = "127.0.0.1";
        int port=31010;
        string plan_filename = "../resources/parquet_scan_union_screen_physical.json";

        if (argc !=1 && argc != 4) {
            std::cout << "Usage: drill_client <server> <port> <plan>\n";
            std::cout << "Example:\n";
            std::cout << "drill_client 127.0.0.1 31010 ../resources/parquet_scan_union_screen_physical.json\n";
            return 1;
        }

        if (argc == 4) {
            drill_addr = argv[1];
            port = atoi(argv[2]);
            plan_filename = argv[3];
        }


        UserServerEndPoint user_server(drill_addr,port);
        asio::io_service io_service;
        DrillClientSync2 client(io_service);
        
        cerr << "Connecting to the Server..." << endl;
        ExecutionContext ctx;
        client.OpenSession(user_server, ctx);

        ifstream f(plan_filename);
        string plan((std::istreambuf_iterator<char>(f)), (std::istreambuf_iterator<char>()));
        plan = "select * from INFORMATION_SCHEMA.SCHEMATA\n";

        RecordBatchBuffer result_buffer;
        ExecutionContext current_ctx;
        
        exec::user::RunQuery drill_query;
        drill_query.set_results_mode(exec::user::STREAM_FULL); // the only mode supported by now
        drill_query.set_type(exec::user::SQL); // set the query type, assuming physical plan
        drill_query.set_plan(plan);

        client.ExecuteStatementDirect(ctx, drill_query, current_ctx, result_buffer);
        client.CloseSession(ctx);
    } catch (std::exception& e) {
        cerr << e.what() << endl;
    }

    return 0;
}

