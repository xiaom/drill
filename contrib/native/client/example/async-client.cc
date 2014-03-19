//#pragma warning (disable: 4996)

#include "common.h"
#include "drill-client-async.h"
#include "rpc-message.h"
using exec::rpc::RpcMode;
using namespace Drill;
#define BOOST_ASIO_ENABLE_HANDLER_TRACKING

int main(int argc, char* argv[]) {
    try {

        string drill_addr = "127.0.0.1";
        int port=31010;
        string plan_filename = "/Users/mx/drill-workspace/incubator-drill/contrib/native/client/resources/parquet_scan_union_screen_physical.json";

        //string s;
        //std::cin >> s ;

        if (argc !=1 && argc != 4) {
            std::cout << "Usage: async_client <server> <port> <plan>\n";
            std::cout << "Example:\n";
            std::cout << "  async_client 127.0.0.1 31010 ../resources/parquet_scan_union_screen_physical.json\n";
            return 1;
        }

        if (argc == 4) {
            drill_addr = argv[1];
            port = atoi(argv[2]);
            plan_filename = argv[3];
        }

        cerr << "Connecting to the Server..." << endl;

        UserServerEndPoint user_server(drill_addr,port);
        asio::io_service io_service;
        DrillClientAsync client(io_service);
        client.Connect(user_server);
        cerr << "Connected!\n" << endl;

        // ---------------------------------------------------------
        // validate handshake

        cerr << "Handshaking..." << endl;
        if (client.ValidateHandShake())
            cerr << "Handshake Successed!\n" << endl;
        // ---------------------------------------------------------

        ifstream f(plan_filename);
        string plan((std::istreambuf_iterator<char>(f)), (std::istreambuf_iterator<char>()));
        cerr << "plan = " << plan << endl;

        client.SubmitQuery(exec::user::PHYSICAL, plan);
        client.GetResult();
        //asio::io_service::work work(io_service);
        io_service.run();
        client.Close();
    } catch (std::exception& e) {
        cerr << e.what() << endl;
    }

    return 0;
}

