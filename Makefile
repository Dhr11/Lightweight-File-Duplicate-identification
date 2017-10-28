CFLAGS = -Wall -Wextra -std=gnu++1z
LIBS = -lboost_system -lboost_filesystem -lcrypto

all: findup

debug: CFLAGS += -DDEBUG -g
debug: findup

findup: src.cpp head.hpp
	g++ $(CFLAGS) src.cpp -o findup $(LIBS)
