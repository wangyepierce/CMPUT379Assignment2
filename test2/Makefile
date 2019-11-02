CC = g++
CFLAGS = -std=c++11 -Wall -Werror -g -O2
SOURCES = $(wildcard *.cpp)
OBJECTS = $(SOURCES:%.cpp=%.o)

all:clean compile wc compress
clean:
	rm -f *.o wordcount result*.txt
compile:
%.o: %.cpp
	${CC} ${CFLAGS} -c $^ -o $@
wc: $(OBJECTS)
	$(CC) -o wordcount $(OBJECTS) -lpthread

compress:
	zip mapreduce.zip *.cpp *.h README.md Makefile
test:
	-rm  result-*.txt
	./wordcount ./a2-testcase/sample*
git:
	git add .
	git commit 