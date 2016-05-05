CC = g++
FLAGS = -std=c++11
OPT = -O3

all: main.o table.o
	$(CC) $(FLAGS) main.o table.o -o db

table.o: table.h table.cc
	$(CC) $(FLAGS) -c table.cc -o table.o

main.o: main.cc
	$(CC) $(FLAGS) -c main.cc -o main.o

clean:
	  rm *.o db
