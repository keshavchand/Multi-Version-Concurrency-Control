flags = -std=c++11 
flags += -ggdb

mvccSim: main.cpp
	@echo Building $@
	g++ $(flags) -o $@ main.cpp

.PHONY: clean
clean:
	rm mvccSim


