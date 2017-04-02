//Server for distributed file system for Advanced Operating Systems
//Author: Matthew Joslin
//UTD Spring 2016

#include <map>
#include <deque>
#include <string>
#include <fstream>
#include <iostream>
#include <time.h>
#include <chrono>
#include <thread>
#include "asio.hpp"

using namespace std;

#define NUM_CLIENTS 7
#define NUM_SERVERS 3
#define LEADER 0
//#define DEBUG

string message_type_string[8] = {"Request", "Acknowledge", "Commit", "Commit Request", "Agree", "", "Finished", "Start"};
string sender_type_string[2] = {"Client - Server", "Peer - Peer"};

//Protocol
int client_to_server = 0;
int peer_to_peer = 1;
int request_message = 0;
int acknowledge_message = 1;
int commit_message = 2;
int commit_request_message = 3;
int agree_message = 4;
int work_message = 5;
int finished_message = 6;
int start_message = 7;
string new_file_cmd = "create";
string delete_file_cmd = "delete";
string move_cmd = "seek";
string read_cmd = "read";
string write_cmd = "write";

//Config
int server_id = 0;
int port = 0;
string working_directory = "data/";
asio::io_service io_service;
string client_name[NUM_CLIENTS];
string server_name[NUM_SERVERS];

class file_info {
public:
    file_info() : file_name(""), file_handle(""), read_contents(""),\
     write_contents(""), offset(0), lock_holder(-1) {}
    file_info(string _file_name)  : file_name(_file_name), file_handle(_file_name),\
     read_contents(""), write_contents(""), offset(0), lock_holder(-1) { }
    file_info(const file_info& file_info)  : file_name(file_info.file_name),\
     file_handle(working_directory + file_info.file_name), read_contents(file_info.read_contents),\
     write_contents(file_info.write_contents), offset(file_info.offset), lock_holder(file_info.lock_holder) { }
    string file_name;
    fstream file_handle;
    string read_contents;
    string write_contents;
    int offset;
    int lock_holder;
};

class client_request {
public:
    client_request() : client_id(0), data_to_write(""),\
     requests_sent(0), agrees_received(0),\
     commit_requests_sent(0), commits_sent(0), acks_sent(0) {}
    client_request(int _client_id, string _data_to_write) : client_id(_client_id),\
     data_to_write(_data_to_write), requests_sent(0), agrees_received(0),\
     commit_requests_sent(0), commits_sent(0), acks_sent(0) { }
    int client_id;
    //File request info
    string data_to_write;
    //Protocol info
    int requests_sent;
    int agrees_received;
    int commit_requests_sent;
    int commits_sent;
    int acks_sent;
};

//File system
map<string, file_info> file_table;
//2-phase commit DS
deque<client_request> request_buffer;

//Temporary variables
int requestor;
string read_contents;
int str_index = 0;
string peer_message = "";
int received_finishes = 0;
bool first_finish = true;
int servers_awake = 1;
int clients_awake = 0;

//Statistics
int total_messages = 0;
int client_total = 0;
string log_file = "total_messages_log.txt";
fstream log_handle;

void close_all_files() {
    for(std::map<string, file_info>::iterator iter = file_table.begin(); iter != file_table.end(); ++iter) {
        if(iter->second.file_handle.is_open()) {
            iter->second.file_handle.close();
        }
    }
}

void my_handler(int s) {
    printf("\n\n\tCaught signal %d!\n\n", s);
    close_all_files();
    exit(0);
}

bool create_new_file(string file_name) {
    if(file_table.find(file_name) != file_table.end()) {
        return false;
    }
    file_info temp(file_name);
    file_table.insert(pair<string, file_info>(file_name, temp));
    file_table.erase(file_name);
    file_table[file_name].file_handle.open (working_directory + file_name, fstream::app);
    file_table[file_name].file_handle.close();
    return true;
}

bool write_file(string file_name, string contents) {
    if(file_table.find(file_name) == file_table.end()) {
        return false;
    }
    file_table[file_name].file_handle.open(working_directory + file_name, fstream::out|fstream::in);
    file_table[file_name].file_handle.seekp(file_table[file_name].offset);
    file_table[file_name].file_handle << contents;
    file_table[file_name].offset = file_table[file_name].file_handle.tellp();
    file_table[file_name].file_handle.close();
    return true;
}

bool delete_file(string file_name) {
    if(file_table.find(file_name) != file_table.end()) {
        file_table.erase(file_name);
    }
    if( remove( string(working_directory + file_name).c_str()) == 0) {
      return true;
    }
    return false;
}

string read_file(string file_name, long length) {
    if(file_table.find(file_name) == file_table.end()) {
        read_contents += "File does not exist. ";
        return "";
    }
    file_table[file_name].file_handle.open(working_directory + file_name, fstream::in);
    file_table[file_name].file_handle.seekg(file_table[file_name].offset, ios::end);
    size_t size = file_table[file_name].file_handle.tellg();
    if(length != 0 && length < (long)size) {
        size = length;
    }
    std::string buffer(size, ' ');
    file_table[file_name].file_handle.seekg(file_table[file_name].offset);
    file_table[file_name].file_handle.read(&buffer[0], size);
    //printf("Read Length:%zu\n", size);
    file_table[file_name].offset = file_table[file_name].file_handle.tellg();
    file_table[file_name].file_handle.close();
    //cout << buffer << endl;
    return buffer;
}

bool set_offset(string file_name, long offset) {
    if(file_table.find(file_name) == file_table.end()) {
       return false;
    }
    file_table[file_name].offset = offset;
    file_table[file_name].file_handle.open (working_directory + file_name, fstream::app);
    file_table[file_name].file_handle.seekp(file_table[file_name].offset);
    file_table[file_name].offset = file_table[file_name].file_handle.tellp();
    file_table[file_name].file_handle.close();
    return true;
}

//We will maintain the invariant that we have consumed the last delimiter
string offset_in_string(string to_parse, char delim) {
    int offset = str_index;
    bool seen_quote = to_parse[offset] == '"';
    if(seen_quote) {
        offset++; //Avoid matching with the same quote
    }
    bool matching_conditions_met = false;
    while(!matching_conditions_met) {
        if(offset >= (int)to_parse.size()) {
            matching_conditions_met = true; //We ran off the end.
        } else if(to_parse[offset] == '"' && seen_quote) {
            matching_conditions_met = true; //We matched the other quote.
        } else if(to_parse[offset] == delim && !seen_quote) {
            matching_conditions_met = true; //We found the delimiter.
        } else {
            offset++; //Just another char.
        }
    }
    string to_return;
    if(seen_quote) {
        to_return = to_parse.substr(str_index + 1, offset - str_index - 1); //We now need to return the string from index + 1 to offset - 1
        offset += 2; //and then adjust offset to + 2
    } else {
        to_return = to_parse.substr(str_index, offset - str_index);
        offset++;
    }
    str_index = offset;
    return to_return;
}

bool is_number(const string& to_test) {
    string::const_iterator it = to_test.begin();
    while (it != to_test.end() && isdigit(*it)) ++it;
    return !to_test.empty() && it == to_test.end();
}

bool send_message(string target_name, string send_buf) {
    try {
        asio::ip::tcp::resolver resolver(io_service);
        asio::ip::tcp::resolver::query query(target_name, to_string(port));
        asio::ip::tcp::resolver::iterator endpoint_iterator = resolver.resolve(query);
        asio::ip::tcp::resolver::iterator end;
        asio::ip::tcp::socket socket(io_service);
        while (endpoint_iterator != end) {
            socket.close();
            socket.connect(*endpoint_iterator++);
        }
        int send_buf_size = send_buf.size();
        send_buf = to_string(send_buf_size) + '\n' + send_buf;
        size_t bytes_written = socket.write_some(asio::buffer(send_buf));
        if(bytes_written != send_buf.size()) {
            printf("Error! Message to server only sent %zu bytes when %zu were supposed to be!\n", bytes_written, send_buf.size());
        }
        socket.shutdown(asio::ip::tcp::socket::shutdown_both);
        socket.close();
        return true;
    } catch (exception& e) {
        cerr << e.what() << endl;
        return false;
    }
}

int handle_request(const char message[]) {
  peer_message = "";
  read_contents = "";
  str_index = 0;
  if(message == NULL) {
      return -1;
  }
  string parsed_message(message);

  string command = offset_in_string(parsed_message, ' ');
  string file_name = offset_in_string(parsed_message, ' ');
  if(strcmp(command.c_str(), new_file_cmd.c_str()) == 0) {
      if(!create_new_file(file_name)) {
          read_contents += "failure -- \"" + file_name + "\" already exists or could not be opened.";
          return -1;
      } else {
          read_contents += "\"" + file_name + "\" was created.";
      }
  } else if(strcmp(command.c_str(), read_cmd.c_str()) == 0) {
      string offset = offset_in_string(parsed_message, ' ');
      long length = 0;
      if(is_number(offset)) {
        length = stol(offset);
      }
      read_contents += "\"" + read_file(file_name, length) + "\"";
  } else if(strcmp(command.c_str(), delete_file_cmd.c_str()) == 0) {
      if(!delete_file(file_name)) {
          read_contents += "\"" + file_name + "\" could not be deleted because it does not exist or is open somewhere else.";
          return -1;
      } else {
          read_contents += "\"" + file_name + "\" is deleted.";
      }
  } else if(strcmp(command.c_str(), move_cmd.c_str()) == 0) {
      string offset = offset_in_string(parsed_message, ' ');
      long index = stol(offset);
      if(!set_offset(file_name, index)) {
          read_contents += "failure -- \"" + file_name + "\" does not exists.";
          return -1;
      } else {
          read_contents += "Cursor is moved to index " + offset + " in \"" + file_name + "\".";
      }
  } else if(strcmp(command.c_str(), write_cmd.c_str()) == 0) {
      string contents = offset_in_string(parsed_message, ' ');
      peer_message = peer_message + "\"" + contents + "\" ";
      if(!write_file(file_name, contents)) {
          read_contents += "Failed to write to \"" + file_name + "\". Maybe the file does not exist.";
          return -1;
      } else {
          read_contents += "A string is written in \"" + file_name + "\".";
      }
  } else {
      read_contents += "Weird command type!";
      return -1;
  }
  return 0;
}

string send_receive_request(string server_name, string send_buf) {
    try {
        asio::ip::tcp::resolver resolver(io_service);
        asio::ip::tcp::resolver::query query(server_name, to_string(port));
        asio::ip::tcp::resolver::iterator endpoint_iterator = resolver.resolve(query);
        asio::ip::tcp::resolver::iterator end;
        asio::ip::tcp::socket socket(io_service);
        while (endpoint_iterator != end) {
            socket.close();
            socket.connect(*endpoint_iterator++);
        }
        size_t bytes_written = socket.write_some(asio::buffer(send_buf));
        if(bytes_written != send_buf.size()) {
            printf("Error! Message to server only sent %zu bytes when %zu were supposed to be!\n", bytes_written, send_buf.size());
        }
        char temp_buffer[1];
        string bytes_str = "";
        size_t bytes_read;
        do {
            bytes_read = socket.read_some(asio::buffer(temp_buffer, 1));
            if(bytes_read == 1 && isdigit(temp_buffer[0])) {
              bytes_str+=temp_buffer[0];
            }
        } while(bytes_read == 1 && isdigit(temp_buffer[0]));
        int message_size = stoi(bytes_str);
        char message[message_size];
        bytes_read = socket.read_some(asio::buffer(message, message_size));
        socket.shutdown(asio::ip::tcp::socket::shutdown_both);
        socket.close();
        return string(message);
    } catch (exception& e) {
        cerr << e.what() << endl;
        return "";
    }
}

deque<client_request>::iterator find_request(int client_id) {
    deque<client_request>::iterator it = request_buffer.begin();
    for (; it != request_buffer.end(); it++) {
      if(it->client_id == client_id) {
        return it;
      }
    }
    return request_buffer.end();
}

//Broadcast methods
void send_requests(int client_id, string rest_of_request) {
    deque<client_request>::iterator it = find_request(client_id);
    if(it == request_buffer.end()) {
      cout << "Error, the request is not buffered.";
    }
    for (int i = 0; i < NUM_SERVERS; i++) {
        if(i != server_id) {
            it->requests_sent++;
            string send_buf = to_string(request_message) + "\t" + to_string(peer_to_peer) + "\t" + to_string(server_id) + "\t" + to_string(client_id) + "\t" + rest_of_request;
            send_message(server_name[i], send_buf);
        }
    }
    it->requests_sent++;
}

void send_commits(int client_id) {
    deque<client_request>::iterator it = find_request(client_id);
    if(it == request_buffer.end()) {
      cout << "Error, the request is not buffered.";
    }
    // cout << "!!!!!!!!Sending commit to " << client_id << "!!!!!!!!!!!!" << endl;
    for (int i = 0; i < NUM_SERVERS; i++) {
        it->commits_sent++;
        string send_buf = to_string(commit_message) + "\t" + to_string(peer_to_peer) + "\t" + to_string(server_id) + "\t" + to_string(client_id) + "\t";
        send_message(server_name[i], send_buf);
    }
}

void handle_receive() {
    asio::ip::tcp::acceptor socket_connection(io_service, asio::ip::tcp::endpoint(asio::ip::tcp::v4(), port));
    asio::ip::tcp::socket socket(io_service);
    stringstream ss;
    string temp;
    int message_parsed;
    int message_length;
    char temp_buffer[1];
    char *message;
    deque<client_request>::iterator curr_request;

    while(true) {
        socket_connection.accept(socket);
        string bytes_str = "";
        size_t bytes_read;
        do {
            bytes_read = socket.read_some(asio::buffer(temp_buffer, 1));
            if(bytes_read == 1 && isdigit(temp_buffer[0])) {
              bytes_str+=temp_buffer[0];
            }
        } while(bytes_read == 1 && isdigit(temp_buffer[0]));
        message_length = stoi(bytes_str);
        message = new char[message_length + 1];
        bytes_read = socket.read_some(asio::buffer(message, message_length));
        if(bytes_read != (size_t)message_length) {
            printf("Missing data\n");
        }
        socket.shutdown(asio::ip::tcp::socket::shutdown_both);
        socket.close();
        message[message_length] = '\0';
        string received_message = string(message);
        delete[] message;
        ss.clear();
        ss.str(received_message);
        temp = "";
        message_parsed = 0;

        getline(ss, temp, '\t'); int message_type = stoi(temp);
        message_parsed +=  1 + temp.size();
        getline(ss, temp, '\t'); int message_sender_type = stoi(temp);
        message_parsed +=  1 + temp.size();
        getline(ss, temp, '\t'); int message_sender_id = stoi(temp);
        message_parsed +=  1 + temp.size();
        getline(ss, temp, '\t'); int client_id = stoi(temp);
        message_parsed +=  1 + temp.size();
        #ifdef DEBUG
          cout << "Message Type: " << message_type_string[message_type] << endl;
          cout << "Message Sender ID: " << message_sender_id << endl;
          cout << "Message Sender Type: " << sender_type_string[message_sender_type] << endl;
          cout << "Client ID: " << client_id << endl;
        #endif
        curr_request = find_request(client_id);

        //Check for message quota
        if(message_sender_type == client_to_server) {
           if(message_type == request_message) {
               string rest_of_request = received_message.substr(message_parsed, received_message.size());
              //  cout << "We think this is the IO portion: " << rest_of_request << endl;
               client_request temp(client_id, rest_of_request);
               temp.agrees_received = 1;
               request_buffer.push_back(temp);

               send_requests(client_id, rest_of_request);
           } else if(message_type == finished_message) {
               received_finishes++;
               if(server_id == 0 && first_finish) {
                   //Inform all servers and clients that we are going down.
                   for (int i = 1; i < NUM_SERVERS; i++) {
                       string send_buf = to_string(finished_message) + "\t" + to_string(peer_to_peer) + "\t" + to_string(server_id) + "\t" + to_string(i) + "\t";
                       //  send_message(server_name[i], send_buf);
                       while(!send_message(server_name[i], send_buf)) {
                        this_thread::sleep_for(chrono::milliseconds(10));
                       }
                   }
                   for (int i = 0; i < NUM_CLIENTS; i++) {
                       if(i != client_id) {
                           string send_buf = to_string(finished_message) + "\t" + to_string(client_to_server) + "\t" + to_string(server_id) + "\t" + to_string(i) + "\t";
                           //  send_message(client_name[i], send_buf);
                           while(!send_message(client_name[i], send_buf)) {
                            this_thread::sleep_for(chrono::milliseconds(10));
                           }
                       }
                   }
               }
               first_finish = false;
               if(received_finishes == NUM_SERVERS + NUM_CLIENTS) {
                   close_all_files();
                   log_handle << "Total Messages: " << to_string(total_messages) << endl;
                   log_handle.close();
                   return;
               }
           } else if(message_type == work_message) {
               string rest_of_request = received_message.substr(message_parsed, received_message.size());
               // cout << "We think this is the IO portion: " << rest_of_request << endl;
               handle_request(rest_of_request.c_str());
           }
        } else if(message_sender_type == peer_to_peer) {
            if(message_type == request_message) {
                string rest_of_request = received_message.substr(message_parsed, received_message.size());
                // cout << "We think this is the IO portion: " << rest_of_request << endl;
                client_request temp(client_id, rest_of_request);
                temp.requests_sent+=NUM_SERVERS;
                request_buffer.push_back(temp);

                string send_buf = to_string(agree_message) + "\t" + to_string(peer_to_peer) + "\t" + to_string(server_id) + "\t" + to_string(client_id) + "\t";
                send_message(server_name[message_sender_id], send_buf);
            } else if(message_type == acknowledge_message) {
                if(curr_request == request_buffer.end()) {
                  cout << "Error, the request is not buffered.";
                }
                curr_request->acks_sent++;
                if(curr_request->acks_sent == 3) {
                    //Send ack to client
                    curr_request->acks_sent++;
                    string send_buf = to_string(acknowledge_message) + "\t"
                      + to_string(client_to_server) + "\t" + to_string(server_id)
                      + "\t" + to_string(client_id) + "\t" + to_string(curr_request->requests_sent) + "\t"
                      + to_string(curr_request->agrees_received) + "\t" + to_string(curr_request->commit_requests_sent) + "\t"
                      + to_string(curr_request->commits_sent) + "\t" + to_string(curr_request->acks_sent) + "\t" ;
                    send_message(client_name[client_id], send_buf);

                    //Log
                    total_messages+=curr_request->requests_sent;
                    total_messages+=curr_request->agrees_received;
                    total_messages+=curr_request->commit_requests_sent;
                    total_messages+=curr_request->commits_sent;
                    total_messages+=curr_request->acks_sent;

                    //We can remove this request
                    curr_request = find_request(client_id);
                    int seen_dups = 0;
                    for(size_t i = 0; i < request_buffer.size(); i++) {
                       if(request_buffer[i].client_id == client_id) {
                         seen_dups++;
                       }
                    }
                    if(seen_dups > 1) {
                        cout << "# Dups: " << seen_dups << endl;
                    }
                    request_buffer.erase(curr_request);
                    //Serve the next request
                    if(!request_buffer.empty() && request_buffer.front().client_id == client_id) {
                        cout << "We either have a duplicate request or we did not remove the original" << endl;
                    }
                    if(!request_buffer.empty() && request_buffer.front().commit_requests_sent != 0) {
                        // cout << "Sending commits from acknowledge block" << endl;
                        send_commits(request_buffer.front().client_id);
                    }
                }
            } else if(message_type == agree_message) {
                if(curr_request == request_buffer.end()) {
                  cout << "Error, the request is not buffered.";
                }
                curr_request->agrees_received++;

                if(curr_request->agrees_received == 3) {
                    string send_buf = to_string(commit_request_message) + "\t" + to_string(peer_to_peer) + "\t" + to_string(server_id) + "\t" + to_string(client_id) + "\t";
                    send_message(server_name[LEADER], send_buf);
                }
            } else if(message_type == commit_message) {
                if(curr_request == request_buffer.end()) {
                  cout << "Error, the request is not buffered.";
                }

                handle_request(curr_request->data_to_write.c_str());

                string send_buf = to_string(acknowledge_message) + "\t" + to_string(peer_to_peer) + "\t" + to_string(server_id) + "\t" + to_string(client_id) + "\t";
                send_message(server_name[LEADER], send_buf);

                if(server_id != LEADER) {
                    request_buffer.erase(curr_request);
                }
            } else if(message_type == commit_request_message) {
                if(curr_request == request_buffer.end()) {
                  cout << "Error, the request is not buffered.";
                }
                curr_request->commit_requests_sent++;
                curr_request->agrees_received = NUM_SERVERS;
                client_request temp = *curr_request;
                request_buffer.erase(curr_request);
                //Add them to the right point in the queue
                deque<client_request>::iterator insertion_point = request_buffer.begin();
                bool insertion_point_found = false;
                for(; insertion_point != request_buffer.end() && !insertion_point_found; insertion_point++) {
                    if(insertion_point->commit_requests_sent != 0) {
                        insertion_point_found = true;
                    }
                }

                if(!insertion_point_found) {
                    request_buffer.push_front(temp);
                } else {
                    request_buffer.insert(insertion_point, temp);
                }

                if(!request_buffer.empty() && request_buffer.front().client_id == client_id) {
                    //Send commit
                    // cout << "Sending commits from commit request block" << endl;
                    send_commits(client_id);
                }
                #ifdef DEBUG
                cout << "\nRequest Buffer: ";
                for (size_t i = 0; i < request_buffer.size(); i++) {
                  cout << "\tID: " << request_buffer[i].client_id << " CRS: " << request_buffer[i].commit_requests_sent << " CS: " << request_buffer[i].commits_sent << "\t";
                }
                cout << endl;
                #endif
            } else if(message_type == finished_message) {
                received_finishes++;
                if(received_finishes == NUM_SERVERS + NUM_CLIENTS) {
                  close_all_files();
                  log_handle << "Total Messages: " << to_string(total_messages) << endl;
                  log_handle.close();
                  return;
                }
            }
        }
    }
}

int main(int argc, char* argv[]) {
    struct sigaction sigIntHandler;
    sigIntHandler.sa_handler = my_handler;
    sigemptyset(&sigIntHandler.sa_mask);
    sigIntHandler.sa_flags = 0;
    sigaction(SIGINT, &sigIntHandler, NULL);
    try {
        if( argc != 2 ) {
            cerr << "Usage: \n\t\tServer ID\n" << endl;
            return 1;
        }
        //size_t bytes_read = 0;
        ifstream settings_file("settings.ini");
        string temp = "";
        getline(settings_file, temp);
        server_id = atoi(argv[1]);
        getline(settings_file, temp);
        port = stoi(temp);
        for(int i = 0; i < NUM_SERVERS; i++) {
            getline(settings_file, server_name[i]);
        }
        getline(settings_file, working_directory);
        working_directory += to_string(server_id) + "/";
        for(int i = 0; i < NUM_CLIENTS; i++) {
            getline(settings_file, client_name[i]);
        }
        log_handle.open (log_file, fstream::out);

        asio::ip::tcp::acceptor socket_connection(io_service, asio::ip::tcp::endpoint(asio::ip::tcp::v4(), port));
        //Initialization: Server 0 is the master
        if(server_id == 0) {
            //Check that all clients and servers are awake.
            int num_awake = 0;
            while(num_awake < (NUM_SERVERS + NUM_CLIENTS - 1)) {
                asio::ip::tcp::socket socket(io_service);

                socket_connection.accept(socket);
                char temp_buffer[1];
                string bytes_str = "";
                size_t bytes_read;
                do {
                    bytes_read = socket.read_some(asio::buffer(temp_buffer, 1));
                    if(bytes_read == 1 && isdigit(temp_buffer[0])) {
                      bytes_str+=temp_buffer[0];
                    }
                } while(bytes_read == 1 && isdigit(temp_buffer[0]));
                int message_length = stoi(bytes_str);
                char message[message_length];
                bytes_read = socket.read_some(asio::buffer(message, message_length));
                string received_message = string(message);
                stringstream ss(received_message);
                string temp;

                int message_parsed = 0;
                getline(ss, temp, '\t'); int message_type = stoi(temp);
                message_parsed +=  1 + temp.size();
                getline(ss, temp, '\t'); int message_sender_type = stoi(temp);
                message_parsed +=  1 + temp.size();
                getline(ss, temp, '\t'); int message_sender_id = stoi(temp);
                message_parsed +=  1 + temp.size();
                getline(ss, temp, '\t');
                message_parsed +=  1 + temp.size();

                if(message_sender_type == peer_to_peer) {
                    servers_awake++;
                    num_awake++;
                    cout << "Received awake from server " << to_string(message_sender_id) << endl;
                } else {
                    if(message_type == start_message) { //Client 0 is responsible for initializing the file
                        num_awake++;
                        cout << "Received awake from client " << to_string(message_sender_id) << endl;
                    } else if(message_type == work_message) {
                        string rest_of_request = received_message.substr(message_parsed, received_message.size());
                        cout << "We think this is the IO portion: " << rest_of_request << endl;
                        handle_request(rest_of_request.c_str());
                    }
                }
                socket.shutdown(asio::ip::tcp::socket::shutdown_both);
                socket.close();
            }
            cout << "Finished waiting for awakes." << endl;
            for(int i = 0; i < NUM_CLIENTS; i++) {
              string send_buf = to_string(start_message) + "\t" + to_string(client_to_server) + "\t" + to_string(server_id) + "\t" + to_string(i) + "\t";
              send_message(client_name[i], send_buf);
            }
        } else {
            printf("Server %d is awake\n", server_id);
            string send_buf = to_string(start_message) + "\t" + to_string(peer_to_peer) + "\t" + to_string(server_id) + "\t" + to_string(server_id) + "\t";
            send_message(server_name[LEADER], send_buf);
        }

        socket_connection.close();

        handle_receive();
    } catch (exception& e) {
        cerr << e.what() << endl;
        close_all_files();
    }
    return 0;
}
