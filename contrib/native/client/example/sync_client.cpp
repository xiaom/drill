//#pragma warning (disable: 4996)

#include "common.h"
#include "client.h"
#include "RpcMessage.h"
using exec::rpc::RpcMode;

struct UserRpcConfig {
    static exec::user::RpcType rpc_type;
    static int rpc_version;
};

int main(int argc, char* argv[])
{
    make_tags();
    try {

        ifstream f("../resources/parquet_scan_union_screen_physical.json");
        string plan((std::istreambuf_iterator<char>(f)), (std::istreambuf_iterator<char>()));


        cerr << "Connecting to the Server..." << endl;
        const string drill_addr = "192.168.202.156";
        const int port=31010;
        UserServerEndPoint user_server(drill_addr,port);


        asio::io_service io_service;
        DrillClient client(io_service);
        client.Connect(user_server);
        cerr << "Connected!\n" << endl;

        RpcMode mode = exec::rpc::REQUEST;
        int rpc_type = exec::user::HANDSHAKE; // initialize as 0
        int coord_id = 1;

        // ---------------------------------------------------------
        // validate handshake
        cerr << "Handshaking..." << endl;
        if (client.ValidateHandShake())
            cerr << "Handshake Successed!\n" << endl;
        // ---------------------------------------------------------

        client.SubmitQuerySync(exec::user::PHYSICAL, plan);
        client.GetResultSync();

        client.Close();
    } catch (std::exception& e) {
        cerr << e.what() << endl;
    }

    return 0;
}

