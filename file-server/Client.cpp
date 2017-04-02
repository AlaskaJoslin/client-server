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
#include <array>
#include <deque>
#include <thread>
#include "asio.hpp"
#include <sys/timeb.h>

using namespace std;

//Constants
#define NUM_CLIENTS 7
#define NUM_SERVERS 3
#define NUM_WRITES 40
#define LEADER 0

//#define DEBUG

//Protocol
string success_message = "successful";
string failure_message = "failure";
string terminate_cmd = "terminate";
string target_file = "target";
int client_to_server = 0;
int request_message = 0;
int acknowledge_message = 1;
int work_message = 5;
int finished_message = 6;
int start_message = 7;

//Config
string client_name[NUM_CLIENTS];
asio::io_service io_service;
string port;
string server_name[NUM_SERVERS];
int client_id = 0;

//Statistics
string log_file = "client_log";
fstream log_handle;
int successful_writes = 0;
int locked_time;
int total_messages = 0;
//Termination
bool termination_conditions_met = false;

void my_handler(int s) {
    printf("\n\n\tCaught signal %d!\n\n", s);
    exit(0);
}

int getMilliCount(){
    timeb tb;
    ftime(&tb);
    int nCount = tb.millitm + (tb.time & 0xfffff) * 1000;
    return nCount;
}

string send_receive_request(string server_name, string send_buf) {
    try {
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
        char message[message_size + 1];
        bytes_read = socket.read_some(asio::buffer(message, message_size));
        socket.shutdown(asio::ip::tcp::socket::shutdown_both);
        socket.close();
        message[message_size] = '\0';
        return string(message);
    } catch(std::exception& e) {
        return "";
    }
}

void automated_send() {
    string send_buf = to_string(request_message) + "\t" + to_string(client_to_server)
        + "\t" + to_string(client_id) + "\t" + to_string(client_id) + "\t";
    string user_request = "write " + target_file + " \"" + to_string(client_id)
        + ", " + to_string(successful_writes) + ", " + client_name[client_id] + "\"";
    send_buf += user_request + ' ';
    int send_buf_size = send_buf.size();
    send_buf = to_string(send_buf_size) + '\n' + send_buf;
    int rand_server = rand() % NUM_SERVERS;
    string parsed_message = send_receive_request(server_name[rand_server], send_buf);
    cout << parsed_message << endl;
}

void send_message(string target_name, string send_buf) {
    try {
        asio::ip::tcp::resolver resolver(io_service);
        asio::ip::tcp::resolver::query query(target_name, port);
        asio::ip::tcp::resolver::iterator endpoint_iterator = resolver.resolve(query);
        asio::ip::tcp::resolver::iterator end;
        asio::ip::tcp::socket socket(io_service);
        while (endpoint_iterator != end) {
            socket.close();
            socket.connect(*endpoint_iterator++);
        }
        int send_buf_size = send_buf.size();
        send_buf = to_string(send_buf_size) + '\n' + send_buf + ' ';
        size_t bytes_written = socket.write_some(asio::buffer(send_buf));
        if(bytes_written != send_buf.size()) {
            printf("Error! Message to server only sent %zu bytes when %zu were supposed to be!\n", bytes_written, send_buf.size());
        }
        socket.shutdown(asio::ip::tcp::socket::shutdown_both);
        socket.close();
    } catch(std::exception& e) {
    }
}

void update_log(double elapsed_time, int requests_sent, int agrees_received, int commit_requests_sent, int commits_sent, int acks_sent) {
    log_handle << "Critical Section Attempt: " << to_string(successful_writes) << endl;
    log_handle << "Time to Enter: " << to_string(elapsed_time) << endl;
    log_handle << "Request Messages: " << to_string(requests_sent) << endl;
    log_handle << "Agree Messages: " << to_string(agrees_received) << endl;
    log_handle << "Commit Request Messages: " << to_string(commit_requests_sent) << endl;
    log_handle << "Commit Messages: " << to_string(commits_sent) << endl;
    log_handle << "Acknowledge Messages: " << to_string(acks_sent) << endl;
    total_messages+=requests_sent;
    total_messages+=agrees_received;
    total_messages+=commit_requests_sent;
    total_messages+=commits_sent;
    total_messages+=acks_sent;
}

void handle_receive() {
    asio::ip::tcp::acceptor socket_connection(io_service, asio::ip::tcp::endpoint(asio::ip::tcp::v4(), stoi(port)));
    asio::ip::tcp::socket socket(io_service);
    string send_buf = to_string(start_message) + "\t" + to_string(client_to_server) + "\t" + to_string(client_id) + "\t" + to_string(client_id) + "\t";
    send_message(server_name[LEADER], send_buf);

    bool start_seen = false;
    while(!start_seen) { //We can start
        socket_connection.accept(socket);
        #ifdef DEBUG
          cout << "Got a server message." << endl;
        #endif
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
        socket.shutdown(asio::ip::tcp::socket::shutdown_both);
        socket.close();
        string received_message = string(message);
        stringstream ss(received_message);
        string temp;
        getline(ss, temp, '\t'); int message_type = stoi(temp);
        getline(ss, temp, '\t'); int message_sender_type = stoi(temp);
        getline(ss, temp, '\t');
        getline(ss, temp, '\t');

        if(bytes_read != (size_t)message_length) {
            printf("Missing data\n");
        }

        if(message_type == start_message && message_sender_type == client_to_server) {
            start_seen = true;
        }
    }

    #ifdef DEBUG
      cout << "Initial client send." << endl;
    #endif
    //Initial request
    int wait_period = (rand() % 401) + 10;
    this_thread::sleep_for(chrono::milliseconds(wait_period));
    locked_time = getMilliCount();
    successful_writes++;
    automated_send();
    #ifdef DEBUG
      cout << "Finished initial client send." << endl;
    #endif

    while( !termination_conditions_met && successful_writes < NUM_WRITES) {
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
        socket.shutdown(asio::ip::tcp::socket::shutdown_both);
        socket.close();
        string received_message = string(message);

        stringstream ss(received_message);
        string temp;

        getline(ss, temp, '\t'); int message_type = stoi(temp);
        getline(ss, temp, '\t');
        getline(ss, temp, '\t');
        getline(ss, temp, '\t');

        if(bytes_read != (size_t)message_length) {
            printf("Missing data\n");
        }
        //Check for message quota: Server 0 is the master
        if(message_type == finished_message) { //Someone finished
           //End and finish log
           termination_conditions_met = true;
           #ifdef DEBUG
             cout << "Got a finished" << endl;
           #endif
        } else if(message_type == acknowledge_message) { //We can start

           locked_time = getMilliCount() - locked_time;
           getline(ss, temp, '\t'); int requests_sent = stoi(temp);
           getline(ss, temp, '\t'); int agrees_received = stoi(temp);
           getline(ss, temp, '\t'); int commit_requests_sent = stoi(temp);
           getline(ss, temp, '\t'); int commits_sent = stoi(temp);
           getline(ss, temp, '\t'); int acks_sent = stoi(temp);

           #ifdef DEBUG
             cout << "Got an acknowledge" << endl;
             cout << "Locked Time: " << locked_time << endl;
             cout << "Requests Sent: " << requests_sent << endl;
             cout << "Agrees Sent: " << agrees_received << endl;
             cout << "Commit Requests Sent: " << commit_requests_sent << endl;
             cout << "Commits Sent: " << commits_sent << endl;
             cout << "Acks Sent: " << acks_sent << endl;
           #endif

           update_log(locked_time, requests_sent, agrees_received, commit_requests_sent, commits_sent, acks_sent);

           int wait_period = (rand() % 401) + 10;
           this_thread::sleep_for(chrono::milliseconds(wait_period));
           locked_time = getMilliCount();

           successful_writes++;
           automated_send();
        } else {
           cout << "Got an unrecognized message from server." << endl;
        }
    }
    send_buf = to_string(finished_message) + "\t" + to_string(client_to_server) + "\t" + to_string(client_id) + "\t" + to_string(client_id) + "\t" + to_string(total_messages) + "\t";
    send_message(server_name[0], send_buf);
    log_handle.close();
}

int main(int argc, char* argv[]) {
    struct sigaction sigIntHandler;
    sigIntHandler.sa_handler = my_handler;
    sigemptyset(&sigIntHandler.sa_mask);
    sigIntHandler.sa_flags = 0;
    sigaction(SIGINT, &sigIntHandler, NULL);
    try {
        if( argc != 2 ) {
            cerr << "Usage: \n\t\tClient ID\n" << endl;
            return 1;
        }
        client_id = atoi(argv[1]);
        log_file = log_file + "_" + to_string(client_id) + ".txt";
        log_handle.open (log_file, fstream::out);
        ifstream settings_file("settings.ini");
        string temp;
        getline(settings_file, temp);
        getline(settings_file, port);
        for(int i = 0; i < NUM_SERVERS; i++) {
            getline(settings_file, server_name[i]);
        }
        getline(settings_file, temp); //Directory
        for(int i = 0; i < NUM_CLIENTS; i++) {
            getline(settings_file, client_name[i]);
        }
        string user_request = "";
        srand (time(NULL));

        if(client_id == 0) {
            for (size_t i = 0; i < NUM_SERVERS; i++) {
                string prefix = to_string(work_message) + "\t" + to_string(client_to_server)
                    + "\t" + to_string(client_id) + "\t" + to_string(client_id) + "\t";
                string user_request = "create " + target_file + " ";
                string send_buf = prefix + user_request + ' ';
                int send_buf_size = send_buf.size();
                send_buf = to_string(send_buf_size) + '\n' + send_buf;
                string parsed_message = send_receive_request(server_name[i], send_buf);
                cout << parsed_message << endl;

                user_request = "delete " + target_file + " ";
                send_buf = prefix + user_request + ' ';
                send_buf_size = send_buf.size();
                send_buf = to_string(send_buf_size) + '\n' + send_buf;
                parsed_message = send_receive_request(server_name[i], send_buf);
                cout << parsed_message << endl;

                user_request = "create " + target_file + " ";
                send_buf = prefix + user_request + ' ';
                send_buf_size = send_buf.size();
                send_buf = to_string(send_buf_size) + '\n' + send_buf;
                parsed_message = send_receive_request(server_name[i], send_buf);
                cout << parsed_message << endl;
            }
        }
        handle_receive();

    } catch (exception& e) {
        cerr << e.what() << endl;
    }
    return 0;
}
