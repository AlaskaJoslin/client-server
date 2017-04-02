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
#include <mutex>
#include <thread>
#include "asio.hpp"
#include <condition_variable>
#include <sys/timeb.h>

using namespace std;

//Constants
#define NUM_CLIENTS 7
#define NUM_SERVERS 3
#define NUM_WRITES 40

//#define DEBUG

//Protocol
string success_message = "successful";
string failure_message = "failure";
string terminate_cmd = "terminate";
string target_file = "target";
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

//Config
array<int, NUM_CLIENTS> sizes = {3, 4, 3, 4, 4, 3, 4};
int quorums[NUM_CLIENTS][4] = {{0, 1, 3, -1}, {1, 2, 3, 6}, {0, 2, 6, -1}, {3, 4, 5, 6}, {1, 2, 4, 5}, {0, 2, 5, -1}, {2, 3, 4, 6}};
string client_name[NUM_CLIENTS];
asio::io_service io_service;
string port;
string server_name[NUM_SERVERS];
int client_id = 0;

//Statistics
string log_file = "client_log";
fstream log_handle;
int request_messages = 0;
int reply_messages = 0;
int release_messages = 0;
int enquire_messages = 0;
int fail_messages = 0;
int yield_messages = 0;
int total_messages = 0;
int successful_writes = 0;

//Termination
bool termination_conditions_met = false;

//Maekawa's algorithm
bool committed = false;
int committed_to = 0;
int number_acknowledges_received = 0;
bool fail_received = false;
bool enquired = false;
typedef struct requestor_infos {
    int id;
    int timestamp;
} requestor_info;
deque<requestor_info> requestors;
deque<int> enquirers;
deque<int> requested;
deque<int> acknowledged;

//Critical Sections
mutex critical_section;
condition_variable made_it_to_crit;
mutex initialized;
bool ready = false;
bool processed = false;

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
        total_messages++;
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
//        cout << "Someone is asleep!" << endl;
        return "";
    }
}

void automated_send() {
#ifdef DEBUG
      cout << "Sending work message to server." << endl;
#endif
      int elapsed_time = getMilliCount();
      string send_buf = to_string(work_message) + "\t" + to_string(elapsed_time) + "\t" + to_string(client_to_server) + "\t" + to_string(client_id) + "\t";
      string user_request = "write " + target_file + " \"" + to_string(client_id)
          + ", " + to_string(successful_writes) + ", " + client_name[client_id] + "\"";
      send_buf += user_request + ' ';
      int send_buf_size = send_buf.size();
      send_buf = to_string(send_buf_size) + '\n' + send_buf;
      int rand_server = rand() % NUM_SERVERS;
      total_messages += NUM_SERVERS * 2;
      string parsed_message = send_receive_request(server_name[rand_server], send_buf);
      cout << parsed_message << endl;
#ifdef DEBUG
      cout << "Finished auto send!" << endl;
#endif
}

void send_message(string target_name, string send_buf) {
    try
    {
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
  //      cout << "Someone is asleep!" << endl;
    }
}

void send_requests() {
    //Send requests to quorum
#ifdef DEBUG
    cout << "Sending requests to quorum." << endl;
#endif
    int elapsed_time = getMilliCount();
    request_messages = 0;
    for (int i = 0; i < sizes[client_id]; i++) {
        request_messages++;
        string send_buf = to_string(request_message) + "\t" + to_string(elapsed_time) + "\t" + to_string(peer_to_peer) + "\t" + to_string(client_id) + "\t";
        send_message(client_name[quorums[client_id][i]], send_buf);
        requested.push_back(quorums[client_id][i]);
    }
}

void send_releases() {
    //Send releases to quorum
#ifdef DEBUG
    cout << "Sending releases to quorum." << endl;
#endif
    int elapsed_time = getMilliCount();
    release_messages = 0;
    requested.clear();
    acknowledged.clear();
    for (int i = 0; i < sizes[client_id]; i++) {
        release_messages++;
        string send_buf = to_string(release_message) + "\t" + to_string(elapsed_time) + "\t" + to_string(peer_to_peer) + "\t" + to_string(client_id) + "\t";
        send_message(client_name[quorums[client_id][i]], send_buf);
    }
}

bool requested_and_acknowledged(int id) {
    bool request = false;
    for (size_t i = 0; i < requested.size(); i++) {
      if(requested[i] == id) {
        request = true;
      }
    }
    bool acknowledge = false;
    for (size_t i = 0; i < acknowledged.size(); i++) {
      if(acknowledged[i] == id) {
        acknowledge = true;
      }
    }
    return request && acknowledge;
}

bool was_requested(int id) {
    bool request = false;
    for (size_t i = 0; i < requested.size(); i++) {
      if(requested[i] == id) {
        request = true;
      }
    }
    return request;
}

void handle_receive() {
    initialized.lock();
    asio::ip::tcp::acceptor socket_connection(io_service, asio::ip::tcp::endpoint(asio::ip::tcp::v4(), stoi(port)));
    asio::ip::tcp::socket socket(io_service);
    while( !termination_conditions_met && successful_writes < NUM_WRITES) {
        critical_section.try_lock();
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
        //Need message type \t timestamp \t client/server \t sender id \t
        stringstream ss(received_message);
        string temp;
        getline(ss, temp, '\t'); int message_type = stoi(temp);
        getline(ss, temp, '\t'); int message_timestamp = stoi(temp);
        getline(ss, temp, '\t'); int message_sender_type = stoi(temp);
        getline(ss, temp, '\t'); int message_sender_id = stoi(temp);
        if(bytes_read != (size_t)message_length) {
            printf("Missing data\n");
        }
#ifdef DEBUG
        cout << "Received Message: " << received_message << endl;
#endif
        //Check for message quota: Server 0 is the master
        if(message_sender_type == client_to_server) {
           if(message_type == finished_message) { //Someone finished
               //End and finish log
#ifdef DEBUG
               cout << "Got finish message from server." << endl;
#endif
               termination_conditions_met = true;
               total_messages++;
               critical_section.unlock();
               {
#ifdef DEBUG
                   cout << "Unlocking critical section!" << endl;
#endif
                   lock_guard<mutex> lk(critical_section);
                   ready = true;
               }
               made_it_to_crit.notify_one();
               //Here the work is actually done
#ifdef DEBUG
               cout << "The work should be done!" << endl;
#endif
               {
                   unique_lock<mutex> lk(critical_section);
                   made_it_to_crit.wait(lk, []{return processed;});
               }
           } else if(message_type == start_message) { //We can start
               initialized.unlock();
               total_messages++;
#ifdef DEBUG
               cout << "Got start message from server." << endl;
#endif
           } else if(message_type == work_message) {
               //Why would we get this? This should happen in the critical_section loop
               printf("Error! We got a work message\n");
           } else {
              cout << "Got an unrecognized message from server." << endl;
           }
        } else if(message_sender_type == peer_to_peer) {
          if(message_type == request_message) { //Request
#ifdef DEBUG
              cout << "Got request message." << endl;
#endif
              if(!committed) {
                  //Send an Acknowledge
#ifdef DEBUG
                  cout << "Not committed, sending an acknowledge." << endl;
#endif
                  committed = true;
                  committed_to = message_sender_id;
                  requestor_info temp;
                  temp.id = message_sender_id;
                  temp.timestamp = message_timestamp;
                  requestors.push_back(temp);
#ifdef DEBUG
                  printf("Requestor Queue\n");
                  for (size_t i = 0; i < requestors.size(); i++) {
                    printf("Requestor %d: %d\n", requestors[i].id, requestors[i].timestamp);
                  }
#endif
                  //Need message type \t timestamp \t client/server \t sender id \t
                  int elapsed_time = getMilliCount();
                  string send_buf = to_string(acknowledge_message) + "\t" + to_string(elapsed_time) + "\t" + to_string(peer_to_peer) + "\t" + to_string(client_id) + "\t";
                  send_message(client_name[message_sender_id], send_buf);
              } else {
                  if(requestors.front().timestamp > message_timestamp || (requestors.front().timestamp == message_timestamp && committed_to > message_sender_id)) {
                      //Send an inquire to the current
                      //If yield, then send ack to this one.
                      if(!enquired) {
#ifdef DEBUG
                          cout << "Already committed, sending an enquire." << endl;
#endif
                          int elapsed_time = getMilliCount();
                          string send_buf = to_string(enquire_message) + "\t" + to_string(elapsed_time) + "\t" + to_string(peer_to_peer) + "\t" + to_string(client_id) + "\t";
                          send_message(client_name[committed_to], send_buf);
                          requestor_info temp_requestor;
                          temp_requestor.id = message_sender_id;
                          temp_requestor.timestamp = message_timestamp;
                          requestors.push_front(temp_requestor);
#ifdef DEBUG
                          printf("Requestor Queue\n");
                          for (size_t i = 0; i < requestors.size(); i++) {
                            printf("Requestor %d: %d\n", requestors[i].id, requestors[i].timestamp);
                          }
#endif
                      } else {
                          int elapsed_time = getMilliCount();
                          string send_buf = to_string(failed_message) + "\t" + to_string(elapsed_time) + "\t" + to_string(peer_to_peer) + "\t" + to_string(client_id) + "\t";
                          send_message(client_name[requestors.front().id], send_buf);
                          requestor_info temp_requestor;
                          temp_requestor.id = message_sender_id;
                          temp_requestor.timestamp = message_timestamp;
                          requestors.push_front(temp_requestor);
#ifdef DEBUG
                          printf("Requestor Queue\n");
                          for (size_t i = 0; i < requestors.size(); i++) {
                            printf("Requestor %d: %d\n", requestors[i].id, requestors[i].timestamp);
                          }
#endif
                      }
                  } else {
                      //This did not win the lotto, add it to the queue
#ifdef DEBUG
                      cout << "Already committed, sending a failed." << endl;
#endif
                      requestor_info temp_requestor;
                      temp_requestor.id = message_sender_id;
                      temp_requestor.timestamp = message_timestamp;
                      deque<requestor_info>::iterator it = requestors.begin();
                      bool inserted = false;
                      for (; it != requestors.end() && !inserted; it++) {
                          if(it->timestamp > message_timestamp || (it->timestamp == message_timestamp && it->id > message_sender_id)) {
                              requestors.insert(it, temp_requestor);
                              inserted = true;
                          }
                      }
                      if(!inserted) {
                          requestors.push_back(temp_requestor);
                      }
#ifdef DEBUG
                      printf("Requestor Queue\n");
                      for (size_t i = 0; i < requestors.size(); i++) {
                        printf("Requestor %d: %d\n", requestors[i].id, requestors[i].timestamp);
                      }
#endif
                      int elapsed_time = getMilliCount();
                      string send_buf = to_string(failed_message) + "\t" + to_string(elapsed_time) + "\t" + to_string(peer_to_peer) + "\t" + to_string(client_id) + "\t";
                      send_message(client_name[message_sender_id], send_buf);
                  }
              }
          } else if(message_type == acknowledge_message) { //Acknowledge
#ifdef DEBUG
              cout << "Got acknowledge message." << endl;
#endif
              number_acknowledges_received++;
#ifdef DEBUG
              printf("Currently holding %d acknowledges\n", number_acknowledges_received);
#endif
              reply_messages++;
              acknowledged.push_back(message_sender_id);
              if(number_acknowledges_received == sizes[client_id]) {
                  critical_section.unlock();
                  {
#ifdef DEBUG
                      cout << "Unlocking critical section!" << endl;
#endif
                      lock_guard<mutex> lk(critical_section);
                      ready = true;
                  }
                  made_it_to_crit.notify_one();
                  //Here the work is actually done
#ifdef DEBUG
                  cout << "The work should be done!" << endl;
#endif
                  {
                      unique_lock<mutex> lk(critical_section);
                      made_it_to_crit.wait(lk, []{return processed;});
                      ready = false;
                  }
              }
          } else if(message_type == release_message) { //Release
#ifdef DEBUG
              cout << "Got release message." << endl;
#endif
              bool found = false;
              for (size_t i = 0; i < requestors.size(); i++) {
                  if(requestors[i].id == message_sender_id) {
                      requestors.erase(requestors.begin() + i);
                      found = true;
                      break;
                  }
              }
              if(!found) { printf("Warning we did not erase!\n"); }
              if(requestors.size() > 0) { //Someone else is waiting
                  int elapsed_time = getMilliCount();
                  committed_to = requestors.front().id;
                  string send_buf = to_string(acknowledge_message) + "\t" + to_string(elapsed_time) + "\t" + to_string(peer_to_peer) + "\t" + to_string(client_id) + "\t";
                  send_message(client_name[committed_to], send_buf);
              } else {
#ifdef DEBUG
                  cout << "We think we aren't committed!" << endl;
#endif
                  committed = false;
              }
#ifdef DEBUG
              printf("Requestor Queue\n");
              for (size_t i = 0; i < requestors.size(); i++) {
                printf("Requestor %d: %d\n", requestors[i].id, requestors[i].timestamp);
              }
#endif
          } else if(message_type == failed_message) { //Failed
              if(was_requested(message_sender_id)) {
#ifdef DEBUG
                  cout << "Got fail message." << endl;
#endif
                  fail_received = true;
                  fail_messages++;
#ifdef DEBUG
                  printf("Enquirers has %zu elements\n", enquirers.size());
#endif
                  for(size_t i = 0; i < enquirers.size(); i++) {
                      yield_messages++;
#ifdef DEBUG
                      cout << "Sending yield to " << enquirers.front() << endl;
#endif
                      deque<int>::iterator it = acknowledged.begin();
                      bool found = false;
                      for (; it != acknowledged.end() && !found; it++) {
                          if(enquirers.front() == *it) {
                              acknowledged.erase(it);
                              found = true;
                          }
                      }
                      if(!found) { printf("Warning we did not erase the enquire sender from acknowledged\n"); }
                      int elapsed_time = getMilliCount();
                      string send_buf = to_string(yield_message) + "\t" + to_string(elapsed_time) + "\t" + to_string(peer_to_peer) + "\t" + to_string(client_id) + "\t";
                      send_message(client_name[enquirers.front()], send_buf);
                      enquirers.pop_front();
                      number_acknowledges_received--;
                  }
              }
          } else if(message_type == yield_message) { //Yield
              //Send an acknowledge to the highest priority
#ifdef DEBUG
              cout << "Got yield message." << endl;
#endif
              enquired = false;
              requestor_info temp_requestor;
              if(committed_to != message_sender_id) { printf("Mismatch!!!!!\n"); }
              deque<requestor_info>::iterator it = requestors.begin();
              bool found = false;
              for (; it != requestors.end(); it++) {
                  if(it->id == message_sender_id) {
                      temp_requestor.id = it->id;
                      temp_requestor.timestamp = it->timestamp;
                      requestors.erase(it);
                      found = true;
                      break;
                  }
              }
              if(!found) {
                printf("Did not find a match for yield.\n");
              } else {
                it = requestors.begin();
                bool inserted = false;
                for (; it != requestors.end() && !inserted; it++) {
                    if(it->timestamp > temp_requestor.timestamp || (it->timestamp == temp_requestor.timestamp && it->id > message_sender_id)) {
                        requestors.insert(it, temp_requestor);
                        inserted = true;
                    }
                }
                if(!inserted) {
                    requestors.push_back(temp_requestor);
#ifdef DEBUG
                    cout << "Warning we took the last chance" << endl;
#endif
                }
#ifdef DEBUG
                printf("Requestor Queue\n");
                for (size_t i = 0; i < requestors.size(); i++) {
                  printf("Requestor %d: %d\n", requestors[i].id, requestors[i].timestamp);
                }
#endif
                int elapsed_time = getMilliCount();
                committed_to = requestors.front().id;
                string send_buf = to_string(acknowledge_message) + "\t" + to_string(elapsed_time) + "\t" + to_string(peer_to_peer) + "\t" + to_string(client_id) + "\t";
                send_message(client_name[committed_to], send_buf);
              }
          } else if(message_type == enquire_message) { //Enquire
              if(requested_and_acknowledged(message_sender_id)) {
#ifdef DEBUG
                  cout << "Got enquire message." << endl;
#endif
                  enquire_messages++;
                  if(fail_received) {
                      //Send a Yield
                      yield_messages++;
                      deque<int>::iterator it = acknowledged.begin();
                      bool found = false;
                      for (; it != acknowledged.end() && !found; it++) {
                          if(message_sender_id == *it) {
                              acknowledged.erase(it);
                              found = true;
                          }
                      }
                      if(!found) { printf("Warning we did not erase the enquire sender from acknowledged\n"); }
#ifdef DEBUG
                      cout << "Sending yield to " << message_sender_id << endl;
#endif
                      number_acknowledges_received--;
                      int elapsed_time = getMilliCount();
                      string send_buf = to_string(yield_message) + "\t" + to_string(elapsed_time) + "\t" + to_string(peer_to_peer) + "\t" + to_string(client_id) + "\t";
                      send_message(client_name[message_sender_id], send_buf);
                  } else {
#ifdef DEBUG
                      printf("Pushed %d onto enquirers\n", message_sender_id);
#endif
                      enquirers.push_back(message_sender_id);
                  }
              }
          }
        }
    }
    if(termination_conditions_met) {
        total_messages++;
        int elapsed_time = getMilliCount();
        string send_buf = to_string(finished_message) + "\t" + to_string(elapsed_time) + "\t" + to_string(client_to_server) + "\t" + to_string(client_id) + "\t" + to_string(total_messages) + "\t";
#ifdef DEBUG
        cout << "Finished, notifying server." << endl;
#endif
        send_message(server_name[0], send_buf);
    }
    log_handle.close();
}

void update_log() {
  log_handle << "Critical Section Attempt: " << to_string(successful_writes) << endl;
  log_handle << "Request Messages: " << to_string(request_messages) << endl;
  log_handle << "Reply Messages: " << to_string(reply_messages) << endl;
  log_handle << "Release Messages: " << to_string(release_messages) << endl;
  log_handle << "Enquire Messages: " << to_string(enquire_messages) << endl;
  log_handle << "Fail Messages: " << to_string(fail_messages) << endl;
  log_handle << "Yield Messages: " << to_string(yield_messages) << endl;

  //TODO log total messages
  total_messages+=request_messages;
  total_messages+=reply_messages;
  total_messages+=release_messages;
  total_messages+=enquire_messages;
  total_messages+=fail_messages;
  total_messages+=yield_messages;

  request_messages = 0;
  reply_messages = 0;
  release_messages = 0;
  enquire_messages = 0;
  fail_messages = 0;
  yield_messages = 0;
}

void critical_section_loop() {
    while( !termination_conditions_met && successful_writes < NUM_WRITES) {
        initialized.lock();
#ifdef DEBUG
        cout << "Made it past initialization." << endl;
#endif
        //Wait a while longer
        int wait_period = rand()*40/RAND_MAX+10;
        this_thread::sleep_for(chrono::milliseconds(wait_period));
        send_requests();
        int request_time = getMilliCount();
#ifdef DEBUG
        cout << "Waiting at critical section." << endl;
#endif

        processed = false;
        std::unique_lock<std::mutex> lk(critical_section);
        made_it_to_crit.wait(lk, []{return ready;});


#ifdef DEBUG
        cout << "Made it into critical section." << endl;
#endif
        int locked_time = getMilliCount();
        locked_time = locked_time - request_time;
        //We made it.
        if(termination_conditions_met) {
            update_log();
            processed = true;
            lk.unlock();
            made_it_to_crit.notify_one();
            return; //We were only let through to exit.
        }
        successful_writes++;
        automated_send();
        if(successful_writes == NUM_WRITES) {
          //We were special
          total_messages++;
          int elapsed_time = getMilliCount();
          string send_buf = to_string(finished_message) + "\t" + to_string(elapsed_time) + "\t" + to_string(client_to_server) + "\t" + to_string(client_id) + "\t" + to_string(total_messages) + "\t";
#ifdef DEBUG
          cout << "Finished, notifying server." << endl;
#endif
          send_message(server_name[0], send_buf);
        } else {
          //Don't let anyone else through
          send_releases();
          //Log to file
          update_log();
#ifdef DEBUG
          log_handle << "Elapsed Time: " << to_string(locked_time) << endl;
#endif
          //Reset vars
          number_acknowledges_received = 0;
          fail_received = false;
          enquirers.clear();

#ifdef DEBUG
          cout << "Finished critical section." << endl;
#endif
        }

        processed = true;
        lk.unlock();
        made_it_to_crit.notify_one();

        initialized.unlock();
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
        thread acceptor(handle_receive);
        if(client_id == 0) {
            int elapsed_time = getMilliCount();
            string prefix = to_string(work_message) + "\t" + to_string(elapsed_time) + "\t" + to_string(client_to_server) + "\t" + to_string(client_id) + "\t";
            string user_request = "create " + target_file + " ";
            string send_buf = prefix + user_request + ' ';
            int send_buf_size = send_buf.size();
            send_buf = to_string(send_buf_size) + '\n' + send_buf;
            total_messages += NUM_SERVERS * 2;
            string parsed_message = send_receive_request(server_name[0], send_buf);
#ifdef DEBUG
            cout << parsed_message << endl;
#endif
            user_request = "delete " + target_file + " ";
            send_buf = prefix + user_request + ' ';
            send_buf_size = send_buf.size();
            send_buf = to_string(send_buf_size) + '\n' + send_buf;
            total_messages += NUM_SERVERS * 2;
            parsed_message = send_receive_request(server_name[0], send_buf);
#ifdef DEBUG
            cout << parsed_message << endl;
#endif
            user_request = "create " + target_file + " ";
            send_buf = prefix + user_request + ' ';
            send_buf_size = send_buf.size();
            send_buf = to_string(send_buf_size) + '\n' + send_buf;
            total_messages += NUM_SERVERS * 2;
            parsed_message = send_receive_request(server_name[0], send_buf);
#ifdef DEBUG
            cout << parsed_message << endl;
            cout << "Finished auto send!" << endl;
#endif
        }
        string send_buf = to_string(start_message) + "\t" + to_string(0) + "\t" + to_string(client_to_server) + "\t" + to_string(client_id) + "\t";
        send_message(server_name[0], send_buf);
        thread requestor(critical_section_loop);
        acceptor.join();
        requestor.join();
    } catch (exception& e) {
        cerr << e.what() << endl;
    }
    return 0;
}
