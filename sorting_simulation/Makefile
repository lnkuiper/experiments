build:
	g++ -std=c++20 -o simulation -O3 simulation.cpp

trace:
	g++ -std=c++20 -o simulation -O3 -D TRACE simulation.cpp

debug:
	g++ -std=c++20 -o simulation -g -fno-omit-frame-pointer -fsanitize=address simulation.cpp

reldebug:
	g++ -std=c++20 -o simulation -g -O3 simulation.cpp

format:
	clang-format -i simulation.cpp fast_mem.hpp

