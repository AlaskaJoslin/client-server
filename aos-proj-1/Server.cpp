//Server for distributed file system for Advanced Operating Systems
//Author: Matthew Joslin
//UTD Spring 2016

#include <map>
#include <string>
#include <fstream>
#include <iostream>
#include "asio.hpp"

using namespace std;

string working_directory = "data/";
string client_to_server = "from client";
string peer_to_peer = "from server";
string success_message = "successful";
string failure_message = "failure";
string new_file_cmd = "create";
string delete_file_cmd = "delete";
string move_cmd = "seek";
string read_cmd = "read";
string write_cmd = "write";
string end_request = "terminate";

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

int port = 0;
string read_contents;
map<string, file_info> file_table;
asio::io_service io_service;

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

bool delete_file(string file_name) {
    if(file_table.find(file_name) != file_table.end()) {
        file_table.erase(file_name);
    }
    if( remove( string(working_directory + file_name).c_str()) == 0) {
      return true;
    }
    return false;
}

//TODO offset
bool write_file(string file_name, string contents) {
    if(file_table.find(file_name) == file_table.end()) {
        return false;
    }
    file_table[file_name].file_handle.open(working_directory + file_name, fstream::out|fstream::in);
    file_table[file_name].file_handle.seekp(file_table[file_name].offset);
    //printf("Requested:%d\nActual:%d\n", file_table[file_name].offset, (int)file_table[file_name].file_handle.tellp());
    file_table[file_name].file_handle << contents;
    file_table[file_name].offset = file_table[file_name].file_handle.tellp();
    file_table[file_name].file_handle.close();
    return true;
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

string requestor;
string command;
string file_name;
string offset;
string contents;
int str_index = 0;

//We will maintain the invariant that we have consumed the last delimiter
string offset_in_string(string to_parse, char delim) {
    int offset = str_index;
    //printf("%c: first char.\n", to_parse[offset]);
    bool seen_quote = to_parse[offset] == '"';
    if(seen_quote) {
        offset++; //Avoid matching with the same quote
    }
    bool matching_conditions_met = false;
    while(!matching_conditions_met) {
        if(offset >= (int)to_parse.size()) {
            //We ran off the end.
            matching_conditions_met = true;
        } else if(to_parse[offset] == '"' && seen_quote) {
            //We matched the other quote.
            matching_conditions_met = true;
        } else if(to_parse[offset] == delim && !seen_quote) {
            //We found the delimiter.
            matching_conditions_met = true;
        } else {
            //Just another char.
            offset++;
        }
    }
    string to_return;
    if(seen_quote) {
        //We now need to return the string from index + 1 to offset - 1
        //and then adjust offset to + 2
        to_return = to_parse.substr(str_index + 1, offset - str_index - 1);
        offset += 2;
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

int handle_request(char message[]) {
  read_contents = "";
  if(message == NULL) {
      return -1;
  }
  string parsed_message(message);
  requestor = offset_in_string(parsed_message, '\n');
  //cout << "Requestor: " << requestor << endl;
  command = offset_in_string(parsed_message, ' ');
  //cout << "Command: " << command << endl;
  if(strcmp(command.c_str(), end_request.c_str()) != 0) {
    file_name = offset_in_string(parsed_message, ' ');
    //cout << "File Name: " << file_name << endl;
  }
  if(strcmp(command.c_str(), read_cmd.c_str()) == 0) {
      offset = offset_in_string(parsed_message, ' ');
      //cout << "Offset:" << offset << "." << endl;
      long length = 0;
      if(is_number(offset)) {
        length = stol(offset);
      }
      read_contents += "\"" + read_file(file_name, length) + "\"";
      str_index = 0;
      return 0;
  } else if(strcmp(command.c_str(), new_file_cmd.c_str()) == 0) {
      if(!create_new_file(file_name)) {
          read_contents += "failure -- \"" + file_name + "\" already exists or could not be opened.";
          str_index = 0;
          return -1;
      } else {
          read_contents += "\"" + file_name + "\" was created.";
          str_index = 0;
          return 0;
      }
  } else if(strcmp(command.c_str(), delete_file_cmd.c_str()) == 0) {
      if(!delete_file(file_name)) {
          read_contents += "\"" + file_name + "\" could not be deleted because it does not exist or is open somewhere else.";
          str_index = 0;
          return -1;
      } else {
          read_contents += "\"" + file_name + "\" is deleted.";
          str_index = 0;
          return 0;
      }
  } else if(strcmp(command.c_str(), move_cmd.c_str()) == 0) {
      offset = offset_in_string(parsed_message, ' ');
      long index = stol(offset);
      //printf("Offset: %ld\n", index);
      if(!set_offset(file_name, index)) {
          read_contents += "failure -- \"" + file_name + "\" does not exists.";
          str_index = 0;
          return -1;
      } else {
          read_contents += "Cursor is moved to index " + offset + " in \"" + file_name + "\".";
          str_index = 0;
          return 0;
      }
  } else if(strcmp(command.c_str(), write_cmd.c_str()) == 0) {
      contents = offset_in_string(parsed_message, ' ');
      //cout << "Contents: " << contents << endl;
      if(!write_file(file_name, contents)) {
          read_contents += "Failed to write to \"" + file_name + "\". Maybe the file does not exist.";
          str_index = 0;
          return -1;
      } else {
          read_contents += "A string is written in \"" + file_name + "\".";
          str_index = 0;
          return 0;
      }
  } else if(strcmp(command.c_str(), end_request.c_str()) == 0) {
      read_contents += "The session is going to be closed.";
      str_index = 0;
      return 1;
  } else {
      read_contents += "Weird command type!";
      str_index = 0;
      return -1;
  }
  str_index = 0;
  return 0;
}

string convert_to_peer_message(char message[]) {
    string parsed_message(message);
    size_t index = 0;
    parsed_message.find(client_to_server, index);
    parsed_message.replace(0, peer_to_peer.size(), peer_to_peer);
    int message_size = parsed_message.size();
    parsed_message = to_string(message_size) + '\n' + parsed_message;
    return parsed_message;
}

string send_receive_request(string server_name, string send_buf) {
    asio::ip::tcp::resolver resolver(io_service);
    asio::ip::tcp::resolver::query query(server_name, to_string(port));
    asio::ip::tcp::resolver::iterator endpoint_iterator = resolver.resolve(query);
    asio::ip::tcp::resolver::iterator end;
    asio::ip::tcp::socket socket(io_service);
    //cout << "Opened socket to peer: " << server_name << endl;
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
        int num_servers = stoi(temp);
        int server_id = atoi(argv[1]);
        getline(settings_file, temp);
        port = stoi(temp);
        string server_name[num_servers];
        for(int i = 0; i < num_servers; i++) {
            getline(settings_file, server_name[i]);
        }
        getline(settings_file, working_directory);
        working_directory += to_string(server_id) + "/";
        //Client facing
        asio::ip::tcp::acceptor socket_connection(io_service, asio::ip::tcp::endpoint(asio::ip::tcp::v4(), port));
        while(true) {
            //TODO put in a thread
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
            //int return_value = 
            handle_request(message);
            string return_message = "";
            if(strcmp(command.c_str(), end_request.c_str()) != 0) {
                return_message = "Server " + to_string(server_id) + ":: " + read_contents;
            } else {
                return_message = read_contents;
            }
            //Peer facing
            if(strcmp(requestor.c_str(), client_to_server.c_str()) == 0 && strcmp(command.c_str(), read_cmd.c_str()) != 0) {
                //cout << "Trying to contact peers." << endl;
                for (int i = 0; i < num_servers; i++) {
                    if(i != server_id) {
                        string temp = send_receive_request(server_name[i], convert_to_peer_message(message));
                        if(strcmp(temp.c_str(), return_message.c_str()) != 0) {
                           //return_message += temp;
                        }
                    }
                }
            }
            int return_message_size = return_message.size();
            return_message = to_string(return_message_size) + "\n" + return_message;
            bytes_written = socket.write_some(asio::buffer(return_message));
            if(bytes_written != return_message.size()) {
                printf("Error! Message to server only sent %zu bytes when %zu were supposed to be!\n", bytes_written, return_message.size());
            }
            /*if(return_value == 1) {
                cout << "Ending session!" << endl;
                close_all_files();
                return 0;
            }*/
        }
    } catch (exception& e) {
        cerr << e.what() << endl;
        close_all_files();
    }
    return 0;
}
