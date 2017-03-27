//Client for distributed file system for Advanced Operating Systems
//Author: Matthew Joslin
//UTD Spring 2016

#include <stdio.h>
#include <signal.h>
#include <stdlib.h>
#include <unistd.h>
#include <iostream>
#include <sstream>
#include <fstream>
#include <string>
#include <time.h>
#include "asio.hpp"

using namespace std;

string client_to_server = "from client";
string success_message = "successful";
string failure_message = "failure";
string terminate_cmd = "terminate";
asio::io_service io_service;
string port;

void my_handler(int s) {
    printf("\n\n\tCaught signal %d!\n\n", s);
    exit(0);
}

void display_menu() {
    printf("\tcreate <filename>\n");
    printf("\tseek <filename> <index>\n");
    printf("\tread <filename> [<length>]\n");
    printf("\twrite <filename> <string>\n");
    printf("\tdelete <filename>\n");
    printf("\tterminate\n");
}

string get_user_input(string description) {
    string line;
    cout << "\tEnter " << description << ": ";
    getline(cin, line);
    return line;
}

string send_receive_request(string server_name, string send_buf) {
    asio::ip::tcp::resolver resolver(io_service);
    asio::ip::tcp::resolver::query query(server_name, port);
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
    size_t bytes_read = 0;
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
        ifstream settings_file("settings.ini");
        string temp;
        getline(settings_file, temp);
        int num_servers = stoi(temp);
        getline(settings_file, port);
        string server_name[num_servers];
        for(int i = 0; i < num_servers; i++) {
            getline(settings_file, server_name[i]);
        }
        string user_request = "";
        srand (time(NULL));
        while( strcmp(user_request.c_str(), terminate_cmd.c_str()) != 0) {
            string send_buf = client_to_server + '\n';
            display_menu();
            user_request = get_user_input("request");
            send_buf += user_request + ' ';
            int send_buf_size = send_buf.size();
            send_buf = to_string(send_buf_size) + '\n' + send_buf;
            int rand_server = rand() % num_servers;
            string parsed_message = send_receive_request(server_name[rand_server], send_buf);
            cout << parsed_message << endl; 
        }
    } catch (exception& e) {
        cerr << e.what() << endl;
    }
    return 0;
}
