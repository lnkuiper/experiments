build:
	c++ -ldl -Wl,-export-dynamic -std=c++20 -o simulation -O3 simulation.cpp
trace:
	c++ -std=c++20 -o simulation -O3 -D TRACE simulation.cpp

debug:
	c++ -std=c++20 -o simulation -g -fno-omit-frame-pointer -fsanitize=address simulation.cpp

reldebug:
	c++ -std=c++20 -o simulation -g -O3 simulation.cpp

format:
	clang-format -i simulation.cpp fast_mem.hpp

