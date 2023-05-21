flags = -std=c++20 -fconcepts -Wall
flags += -ggdb

mvccSim: main.cpp
	@echo Building $@
	g++ $(flags) -o $@ main.cpp

.PHONY: clean test testdbg

clean:
	rm mvccSim a.out

test: main.cpp test.cpp
	g++ $(flags) -o test main.cpp -DTESTING
	./test
	rm ./test

testdbg: main.cpp test.cpp
	g++ $(flags) -o test main.cpp -DTESTING
	gdb ./test
	rm ./test


