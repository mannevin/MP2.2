#include <bits/fs_fwd.h>
#include <ctime>

#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>
#include <chrono>
#include <sys/stat.h>
#include <sys/types.h>
#include <vector>
#include <unordered_set>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <mutex>
#include <stdlib.h>
#include <unistd.h>
#include <algorithm>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>

#include "sns.grpc.pb.h"
#include "sns.pb.h"
#include "coordinator.grpc.pb.h"
#include "coordinator.pb.h"


namespace fs = std::filesystem;

using google::protobuf::Timestamp;
using google::protobuf::Duration;
using grpc::Server;
using grpc::ClientContext;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
using csce438::CoordService;
using csce438::ServerInfo;
using csce438::Confirmation;
using csce438::ID;
using csce438::ServerList;
using csce438::SynchService;
using csce438::AllUsers;
// tl = timeline, fl = follow list
using csce438::TLFL;

int synchID = 1;
int clusterID = 1;
std::vector<std::string> get_lines_from_file(std::string);
void run_synchronizer(std::string,std::string,std::string,int);
std::vector<std::string> get_all_users_func(int);
std::vector<std::string> get_tl_or_fl(int, int, bool);

std::unique_ptr<csce438::CoordService::Stub> coordinator_stub_;
std::vector<std::string> remove_dups(std::vector<std::string> vec) {
    std::sort(vec.begin(), vec.end());
    vec.erase(std::unique(vec.begin(), vec.end()), vec.end());
    return vec;
}
class SynchServiceImpl final : public SynchService::Service {
    Status GetAllUsers(ServerContext* context, const Confirmation* confirmation, AllUsers* allusers) override{
        //std::cout<<"Got GetAllUsers"<<std::endl;
        std::vector<std::string> list = get_all_users_func(synchID);

        //package list
        for(auto s:list){
            allusers->add_users(s);
        }

        //return list
        return Status::OK;
    }

    Status GetTLFL(ServerContext* context, const ID* id, TLFL* tlfl){
        //std::cout<<"Got GetTLFL"<<std::endl;
        int clientID = id->id();

        std::vector<std::string> tl = get_tl_or_fl(synchID, clientID, true);
        std::vector<std::string> fl = get_tl_or_fl(synchID, clientID, false);

        //now populate TLFL tl and fl for return
        for(auto s:tl){
            tlfl->add_tl(s);
        }
        for(auto s:fl){
            tlfl->add_fl(s);
        }
        tlfl->set_status(true); 

        return Status::OK;
    }

    Status ResynchServer(ServerContext* context, const ServerInfo* serverinfo, Confirmation* c){
        std::cout<<serverinfo->type()<<"("<<serverinfo->serverid()<<") just restarted and needs to be resynched with counterpart"<<std::endl;
        std::string backupServerType;

        // YOUR CODE HERE


        return Status::OK;
    }
};

void RunServer(std::string coordIP, std::string coordPort, std::string port_no, int synchID){
  //localhost = 127.0.0.1
  std::string server_address("127.0.0.1:"+port_no);
  SynchServiceImpl service;
  //grpc::EnableDefaultHealthCheckService(true);
  //grpc::reflection::InitProtoReflectionServerBuilderPlugin();
  ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&service);
  // Finally assemble the server.
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;

  std::thread t1(run_synchronizer,coordIP, coordPort, port_no, synchID);
  /*
  TODO List:
    -Implement service calls
    -Set up initial single heartbeat to coordinator
    -Set up thread to run synchronizer algorithm
  */

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}



int main(int argc, char** argv) {

    int opt = 0;
    std::string coordIP;
    std::string coordPort;
    std::string port = "3029";

    while ((opt = getopt(argc, argv, "h:k:p:i:")) != -1){
        switch(opt) {
            case 'h':
                coordIP = optarg;
                break;
            case 'k':
                coordPort = optarg;
                break;
            case 'p':
                port = optarg;
                break;
            case 'i':
                synchID = std::stoi(optarg);
                clusterID = synchID;
                if (synchID > 3) {
                    clusterID = synchID - 3;
                }
                break;
            default:
                std::cerr << "Invalid Command Line Argument\n";
        }
    }

    RunServer(coordIP, coordPort, port, synchID);
    return 0;
}

void run_synchronizer(std::string coordIP, std::string coordPort, std::string port, int synchID){
    //setup coordinator stub
    //std::cout<<"synchronizer stub"<<std::endl;
    std::string target_str = coordIP + ":" + coordPort;
    std::unique_ptr<CoordService::Stub> coord_stub_;
    coord_stub_ = std::unique_ptr<CoordService::Stub>(CoordService::NewStub(grpc::CreateChannel(target_str, grpc::InsecureChannelCredentials())));
    //std::cout<<"MADE STUB"<<std::endl;
    std::cout << "Set up coordinator stub" << std::endl;
    ServerInfo msg;
    Confirmation c;
    grpc::ClientContext context;

    msg.set_serverid(synchID);
    msg.set_hostname("127.0.0.1");
    msg.set_port(port);
    msg.set_type("follower");

    //send init heartbeat
    coord_stub_->Heartbeat(&context, msg, &c);
    //TODO: begin synchronization process
    while(true){
        //change this to 30 eventually
        sleep(20);
        //synch all users file 
            //get list of all followers
        ServerList sl;
        ClientContext cc;
        std::cout << "Got all synch servers" << std::endl;
        coord_stub_->AllSynchServers(&cc, msg, &sl);

            // YOUR CODE HERE
            //set up stub
            //send each a GetAllUsers request
            AllUsers au;
            std::vector<std::string> allUsers;

            //aggregate users into a list
            //sort list and remove duplicates
            for (int i = 0; i < sl.serverid_size(); i++) {
                std::string follower_addr = sl.hostname(i) + ":" + sl.port(i);
                std::cout << "Got follower address at: " << follower_addr << std::endl;
                std::unique_ptr<SynchService::Stub> sync_stub_ = std::unique_ptr<SynchService::Stub>(SynchService::NewStub(grpc::CreateChannel(follower_addr, grpc::InsecureChannelCredentials())));
                ClientContext cc;
                Confirmation conf;
                sync_stub_->GetAllUsers(&cc, c, &au);
                for (int j = 0; j < au.users_size(); j++) {
                    allUsers.push_back(au.users(j));
                }

                
            }
            allUsers = remove_dups(allUsers);
            // YOUR CODE HERE
            std::cout << "All users sorted" << std::endl;
            //for all the found users
            //if user not managed by current synch
            // ...
            std::set<std::string> managedUsers;
            std::unique_ptr<SynchService::Stub> sync_stub_ = std::unique_ptr<SynchService::Stub>(SynchService::NewStub(grpc::CreateChannel("127.0.0.1:"+port, grpc::InsecureChannelCredentials())));
            Confirmation conf;
            ClientContext cc2;
            // sync_stub_->GetAllUsers(&cc2, c, &au);
            std::vector<std::string> tempVec = get_lines_from_file("./cluster"+std::to_string(clusterID)+"/1/cluster_users.txt");
            for (int j = 0; j < tempVec.size(); j++) {
                managedUsers.insert(tempVec[j]);
            }
            
            for (int i = 0; i < allUsers.size(); i++) {
                
                if (managedUsers.find(allUsers[i]) != managedUsers.end()) {
                    std::cout << "Managed User: " << allUsers[i] << std::endl;
                    std::string currFileNameMaster = "./cluster"+std::to_string(clusterID)+"/" + std::to_string(i) + "_follow_list.txt";

                    std::vector<std::string> following_users = remove_dups(get_lines_from_file(currFileNameMaster));
                    for (std::string following : following_users) {
                        int currId = std::stoi(following);
                        int intClusterId = ((currId - 1) % 3) + 1;
                        // set id
                        ClientContext context;
                        ID id; 
                        ServerInfo serverinfo;
                        id.set_id(intClusterId);
                        coord_stub_->GetSynchronizer(&context, id, &serverinfo);
                        std::string currAddr = serverinfo.hostname() + ":" + serverinfo.port();
                        
                    }

                } else {
                    std::cout << "User: " << allUsers[i] << std::endl;
                    // Get check if user is following one of the managed users
                    int currId = std::stoi(allUsers[i]);
                    int intClusterId = ((currId - 1) % 3) + 1;
                    ID id; 
                    ServerInfo serverinfo;
                    id.set_id(intClusterId);
                    ClientContext currCC;
                    coord_stub_->GetSynchronizer(&currCC, id, &serverinfo);
                    std::string currAddr = serverinfo.hostname() + ":" + serverinfo.port();

                    std::cout<<currAddr << std::endl;
                    sync_stub_ = std::unique_ptr<SynchService::Stub>(SynchService::NewStub(grpc::CreateChannel(currAddr, grpc::InsecureChannelCredentials())));
                    TLFL tlfl;
                    ID tempId;
                    ClientContext currCC2;
                    tempId.set_id(currId);
                    sync_stub_->GetTLFL(&currCC2, tempId, &tlfl);
                    for (int z = 0; z < tlfl.fl_size(); z++) {
                        if (managedUsers.find(tlfl.fl(z)) != managedUsers.end()) {
                            int temp = ((std::stoi(tlfl.fl(z)) - 1) % 3) + 1;
                            std::string followersFileMaster = "./cluster"+std::to_string(temp)+"/1/"+tlfl.fl(z)+".txt";
                            std::string followersFileSlave = "./cluster"+std::to_string(temp)+"/2/"+tlfl.fl(z)+".txt";
                            std::ofstream fileMaster(followersFileMaster, std::ios_base::app);
                            std::ofstream fileSlave(followersFileSlave, std::ios_base::app);
                            if (fileMaster.is_open()) {
                                fileMaster.seekp(0, std::ios_base::beg);
                                fileMaster << std::to_string(i) << "\n";
                                fileSlave << std::to_string(i) << "\n";
                                fileMaster.close();
                                fileSlave.close();
                            }
                        }
                    }
                }
                // std::string slave_prefix = "./cluster" + 
            }
            // YOUR CODE HERE

	    //force update managed users from newly synced users
            //for all users
            // for(auto i : aggregated_users){
            //     //get currently managed users
            //     //if user IS managed by current synch
            //         //read their follower lists
            //         //for followed users that are not managed on cluster
            //         //read followed users cached timeline
            //         //check if posts are in the managed tl
            //         //add post to tl of managed user    
            
            //          // YOUR CODE HERE
            //         }
            //     }
            // }
    }
    return;
}

std::vector<std::string> get_lines_from_file(std::string filename){
  std::vector<std::string> users;
  std::string user;
  std::ifstream file; 
  file.open(filename);
  if(file.peek() == std::ifstream::traits_type::eof()){
    //return empty vector if empty file
    //std::cout<<"returned empty vector bc empty file"<<std::endl;
    file.close();
    return users;
  }
  while(file){
    getline(file,user);

    if(!user.empty())
      users.push_back(user);
  } 

  file.close();

  //std::cout<<"File: "<<filename<<" has users:"<<std::endl;
  /*for(int i = 0; i<users.size(); i++){
    std::cout<<users[i]<<std::endl;
  }*/ 

  return users;
}

bool file_contains_user(std::string filename, std::string user){
    std::vector<std::string> users;
    //check username is valid
    users = get_lines_from_file(filename);
    for(int i = 0; i<users.size(); i++){
      //std::cout<<"Checking if "<<user<<" = "<<users[i]<<std::endl;
      if(user == users[i]){
        //std::cout<<"found"<<std::endl;
        return true;
      }
    }
    //std::cout<<"not found"<<std::endl;
    return false;
}

std::vector<std::string> get_all_users_func(int synchID){
    //read all_users file master and client for correct serverID
    std::string master_users_file = "./cluster"+std::to_string(clusterID)+"/1/all_users.txt";
    // std::string slave_users_file = "./cluster"+std::to_string(synchID)+"/2/all_users";
    //take longest list and package into AllUsers message
    std::vector<std::string> master_user_list = get_lines_from_file(master_users_file);
    // std::vector<std::string> slave_user_list = get_lines_from_file(slave_users_file);

    // if(master_user_list.size() >= slave_user_list.size())
    //     return master_user_list;
    // else
    //     return slave_user_list;
    return master_user_list;
}

std::vector<std::string> get_tl_or_fl(int synchID, int clientID, bool tl){
    std::string master_fn = "./cluster"+std::to_string(clusterID)+"/1/"+std::to_string(clientID);
    std::string slave_fn = "./cluster"+std::to_string(clusterID)+"/2/" + std::to_string(clientID);
    if(tl){
        master_fn.append(".txt");
        slave_fn.append(".txt");
    }else{
        master_fn.append("_follow_list");
        slave_fn.append("_follow_list");
    }

    std::vector<std::string> m = get_lines_from_file(master_fn);
    std::vector<std::string> s = get_lines_from_file(slave_fn);

    if(m.size()>=s.size()){
        return m;
    }else{
        return s;
    }

}
