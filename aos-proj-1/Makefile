CXX = g++
CXXFLAGS = -std=c++11 -Wall -g -Iasio
LIB = -pthread
OS = $(shell uname)

all: SERVER CLIENT

SERVER:
	$(CXX) $(CXXFLAGS) Server.cpp -o server $(LIB)

CLIENT:
	$(CXX) $(CXXFLAGS) Client.cpp -o client $(LIB)

clean:
	rm -f *~ server client
