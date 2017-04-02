//Server for distributed file system for Advanced Operating Systems
//Author: Matthew Joslin
//UTD Spring 2016

#include <map>
#include <string>
#include <fstream>
#include <iostream>
#include <time.h>
#include "asio.hpp"

using namespace std;

#define NUM_CLIENTS 7
#define NUM_SERVERS 3

//#define DEBUG

//Protocol
int client_to_server = 0;
int peer_to_peer = 1;
int request_message = 0;
int acknowledge_message = 1;
int release_message = 2;
int failed_message = 3;
int yield_message = 4;
int enquire_message = 5;
int finished_message = 6;
int start_message = 7;
int work_message = 8;
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
    file_info() : file_name(""), file_handle(""), read_contents(""), write_contents(""), offset(0), lock_holder(-1) {}
    file_info(string _file_name)  : file_name(_file_name), file_handle(_file_name), read_contents(""), write_contents(""), offset(0), lock_holder(-1) { }
    file_info(const file_info& file_info)  : file_name(file_info.file_name), file_handle(working_directory + file_info.file_name), read_contents(file_info.read_contents), write_contents(file_info.write_contents), offset(file_info.offset), lock_holder(file_info.lock_holder) { }
    string file_name;
    fstream file_handle;
    string read_contents;
    string write_contents;
    int offset;
    int lock_holder;
};

//File system
map<string, file_info> file_table;

//Temporary variables
int requestor;
string command;
string file_name;
string offset;
string contents;
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
    for(std::map<string, file_info>::iterator iter = file_table.begin(); iter != file_table.end(); ++iter)
    {
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

void send_message(string target_name, string send_buf) {
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
}

int handle_request(char message[]) {
  peer_message = "";
  read_contents = "";
  str_index = 0;
  if(message == NULL) {
      return -1;
  }
  string parsed_message(message);
  int message_type = stoi(offset_in_string(parsed_message, '\t'));
  peer_message = to_string(message_type) + "\t";
  int message_timestamp = stoi(offset_in_string(parsed_message, '\t'));
  peer_message = peer_message + to_string(message_timestamp) + "\t";
  requestor = stoi(offset_in_string(parsed_message, '\t'));
  peer_message = peer_message + to_string(peer_to_peer) + "\t";
  int message_sender_id = stoi(offset_in_string(parsed_message, '\t'));
  peer_message = peer_message + to_string(message_sender_id) + "\t";
  if(message_type == finished_message) {
      client_total = stoi(offset_in_string(parsed_message, '\t'));
      log_handle << "Total Messages for Client " << to_string(message_sender_id) << ": " << to_string(client_total) << endl;
      total_messages += client_total;
#ifdef DEBUG
      std::cout << "Received terminate!" << std::endl;
#endif
      received_finishes++;
      if(server_id == 0 && first_finish) {
          //Inform all servers and clients that we are going down.
          for (int i = 1; i < NUM_SERVERS; i++) {
              send_message(server_name[i], message);
          }
          for (int i = 0; i < NUM_CLIENTS; i++) {
            if(i != message_sender_id) {
              send_message(client_name[i], message);
            }
          }
#ifdef DEBUG
          cout << "Broadcasting terminate!" << std::endl;
#endif
      }
      first_finish = false;
      read_contents += "The session is going to be closed.";
      return 1;
  }

  if(message_type == work_message) {
      command = offset_in_string(parsed_message, ' ');
      peer_message = peer_message + command;
      file_name = offset_in_string(parsed_message, ' ');
      peer_message = peer_message + " \"" + file_name + "\" ";
      if(strcmp(command.c_str(), new_file_cmd.c_str()) == 0) {
          if(!create_new_file(file_name)) {
              read_contents += "failure -- \"" + file_name + "\" already exists or could not be opened.";
              return -1;
          } else {
              read_contents += "\"" + file_name + "\" was created.";
          }
      } else if(strcmp(command.c_str(), read_cmd.c_str()) == 0) {
          offset = offset_in_string(parsed_message, ' ');
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
          offset = offset_in_string(parsed_message, ' ');
          long index = stol(offset);
          if(!set_offset(file_name, index)) {
              read_contents += "failure -- \"" + file_name + "\" does not exists.";
              return -1;
          } else {
              read_contents += "Cursor is moved to index " + offset + " in \"" + file_name + "\".";
          }
      } else if(strcmp(command.c_str(), write_cmd.c_str()) == 0) {
          contents = offset_in_string(parsed_message, ' ');
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
  }
  return 0;
}

string send_receive_request(string server_name, string send_buf) {
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
        size_t bytes_read = 0;
        size_t bytes_written = 0;
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
                //Need message type \t timestamp \t client/server \t sender id \t
                stringstream ss(received_message);
                string temp;
                getline(ss, temp, '\t'); int message_type = stoi(temp);
                getline(ss, temp, '\t');
                getline(ss, temp, '\t'); int message_sender_type = stoi(temp);
                getline(ss, temp, '\t'); int message_sender_id = stoi(temp);

                if(message_sender_type == peer_to_peer) {
                    servers_awake++;
                    num_awake++;
//#ifdef DEBUG
                    cout << "Received awake from server " << to_string(message_sender_id) << endl;
//#endif
                } else {
                    if(message_sender_id == 0 && message_type != start_message) { //Client 0 is responsible for initializing the file
                        if(servers_awake == NUM_SERVERS) {
                            if(handle_request(message) == 0) {
                              string return_message = read_contents;
                              int peer_message_size = peer_message.size();
                              peer_message = to_string(peer_message_size) + '\n' + peer_message;
                              for (int i = 0; i < NUM_SERVERS; i++) {
                                  if(i != server_id) {
#ifdef DEBUG
                                      cout << "Broadcasting to " << server_name[i] << endl;
#endif
                                      string temp = send_receive_request(server_name[i], peer_message);
                                  }
                              }
                              int return_message_size = return_message.size();
                              return_message = to_string(return_message_size) + "\n" + return_message;
                              bytes_written = socket.write_some(asio::buffer(return_message));
                              if(bytes_written != return_message.size()) {
                                printf("Error! Message to server only sent %zu bytes when %zu were supposed to be!\n", bytes_written, return_message.size());
                              }
#ifdef DEBUG
                              cout << "Server finished processing message!" << endl;
#endif
                            } else {
                              printf("Error in handling file initialization.\n");
                            }
                        } else { printf("Error. The servers are not awake.\n"); }
                    } else {
                        num_awake++;
//#ifdef DEBUG
                        cout << "Received awake from client " << to_string(message_sender_id) << endl;
//#endif
                    }
                }
                socket.shutdown(asio::ip::tcp::socket::shutdown_both);
                socket.close();
            }
//#ifdef DEBUG
            cout << "Finished waiting for awakes." << endl;
//#endif
            for(int i = 0; i < NUM_CLIENTS; i++) {
              string send_buf = to_string(start_message) + "\t" + to_string(0) + "\t" + to_string(client_to_server) + "\t" + to_string(server_id) + "\t";
              send_message(client_name[i], send_buf);
            }
        } else {
//#ifdef DEBUG
            printf("Server %d is awake\n", server_id);
//#endif
            string send_buf = to_string(start_message) + "\t" + to_string(0) + "\t" + to_string(peer_to_peer) + "\t" + to_string(server_id) + "\t";
            send_message(server_name[0], send_buf);
        }

        while(true) {
            asio::ip::tcp::socket socket(io_service);
            socket_connection.accept(socket);
            char temp_buffer[1];
            string bytes_str = "";
            do {
                bytes_read = socket.read_some(asio::buffer(temp_buffer, 1));
                if(bytes_read == 1 && isdigit(temp_buffer[0])) {
                  bytes_str+=temp_buffer[0];
                }
            } while(bytes_read == 1 && isdigit(temp_buffer[0]));
            int message_length = stoi(bytes_str);
            char message[message_length];
            bytes_read = socket.read_some(asio::buffer(message, message_length));
            if(bytes_read != (size_t)message_length) {
                printf("Missing data\n");
            }
#ifdef DEBUG
            printf("Received message at server: %s\n", message);
#endif
            int return_value = handle_request(message);
            if(return_value == 1) {
                if(server_id == 0 && received_finishes == NUM_CLIENTS) {
#ifdef DEBUG
                  cout << "Ending session!" << endl;
#endif
                  close_all_files();
                  log_handle << "Total Messages: " << to_string(total_messages) << endl;
                  log_handle.close();
                  return 0;
                } else if(server_id != 0) {
                  close_all_files();
                  return 0;
                }
            } else {
                string return_message = read_contents;
                //Peer facing
                if(requestor == client_to_server && strcmp(command.c_str(), read_cmd.c_str()) != 0) {
                    int peer_message_size = peer_message.size();
                    peer_message = to_string(peer_message_size) + '\n' + peer_message;
                    for (int i = 0; i < NUM_SERVERS; i++) {
                        if(i != server_id) {
#ifdef DEBUG
                            cout << "Broadcasting to " << server_name[i] << endl;
#endif
                            string temp = send_receive_request(server_name[i], peer_message);
                        }
                    }
                }
                int return_message_size = return_message.size();
                return_message = to_string(return_message_size) + "\n" + return_message;
                bytes_written = socket.write_some(asio::buffer(return_message));
                if(bytes_written != return_message.size()) {
                    printf("Error! Message to server only sent %zu bytes when %zu were supposed to be!\n", bytes_written, return_message.size());
                }
                socket.shutdown(asio::ip::tcp::socket::shutdown_both);
                socket.close();
#ifdef DEBUG
                cout << "Server finished processing message!" << endl;
#endif
            }
        }
    } catch (exception& e) {
        cerr << e.what() << endl;
        close_all_files();
    }
    return 0;
}
