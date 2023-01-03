#include <cinttypes>

extern "C" {

bool CompareIntegersLT(const uint32_t &lhs, const uint32_t &rhs) {
	return lhs < rhs;
}

bool CompareIntegersEQ(const uint32_t &lhs, const uint32_t &rhs) {
	return lhs == rhs;
}
}
