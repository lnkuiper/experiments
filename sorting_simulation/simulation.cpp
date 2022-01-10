#include "fast_mem.hpp"
#include "pdqsort.h"

#include <algorithm>
#include <assert.h>
#include <chrono>
#include <compare>
#include <cstring>
#include <fstream>
#include <iostream>
#include <limits>
#include <memory>
#include <random>
#include <sstream>
#include <thread>
#include <vector>

using namespace std;
using namespace chrono;

#define RANDOM_SEED          42
#define STANDARD_VECTOR_SIZE 1024

#define BSWAP16(x) ((uint16_t)((((uint16_t)(x)&0xff00) >> 8) | (((uint16_t)(x)&0x00ff) << 8)))

#define BSWAP32(x)                                                                                                     \
	((uint32_t)((((uint32_t)(x)&0xff000000) >> 24) | (((uint32_t)(x)&0x00ff0000) >> 8) |                               \
	            (((uint32_t)(x)&0x0000ff00) << 8) | (((uint32_t)(x)&0x000000ff) << 24)))

#define BSWAP64(x)                                                                                                     \
	((uint64_t)((((uint64_t)(x)&0xff00000000000000ull) >> 56) | (((uint64_t)(x)&0x00ff000000000000ull) >> 40) |        \
	            (((uint64_t)(x)&0x0000ff0000000000ull) >> 24) | (((uint64_t)(x)&0x000000ff00000000ull) >> 8) |         \
	            (((uint64_t)(x)&0x00000000ff000000ull) << 8) | (((uint64_t)(x)&0x0000000000ff0000ull) << 24) |         \
	            (((uint64_t)(x)&0x000000000000ff00ull) << 40) | (((uint64_t)(x)&0x00000000000000ffull) << 56)))

typedef uint8_t data_t;
typedef data_t *data_ptr_t;
typedef uint64_t idx_t;

//===--------------------------------------------------------------------===//
// General
//===--------------------------------------------------------------------===//
idx_t CurrentTime() {
	return duration_cast<nanoseconds>(high_resolution_clock::now().time_since_epoch()).count();
}

string CreateOutput(string category, vector<idx_t> results, idx_t columns) {
	while (results.size() < columns - 1) {
		results.push_back(0);
	}
	ostringstream output;
	output << category;
	for (idx_t i = 0; i < results.size(); i++) {
		output << "," << results[i];
	}
	return output.str();
}

unique_ptr<data_t[]> InitRowIDs(const idx_t &count, bool shuf) {
	auto result = unique_ptr<data_t[]>(new data_t[count * sizeof(uint32_t)]);
	uint32_t *order = (uint32_t *)result.get();
	for (uint32_t i = 0; i < count; i++) {
		order[i] = i;
	}
	if (shuf) {
		shuffle(order, order + count, default_random_engine(RANDOM_SEED));
	}
	return result;
}

vector<unique_ptr<data_t[]>> AllocateColumns(const idx_t &count, const idx_t &columns, const idx_t &col_width) {
	vector<unique_ptr<data_t[]>> result;
	for (idx_t column = 0; column < columns; column++) {
		result.push_back(unique_ptr<data_t[]>(new data_t[count * col_width]));
	}
	return result;
}

unique_ptr<data_t[]> AllocateRows(const idx_t &count, const idx_t &row_width) {
	return unique_ptr<data_t[]>(new data_t[count * row_width]);
}

template <class T>
void FillColumns(vector<unique_ptr<data_t[]>> &columns, const idx_t &count, string datagen) {
	for (idx_t column = 0; column < columns.size(); column++) {
		T *col_ptr = (T *)columns[column].get();
		if (datagen == "deterministic") {
			const idx_t max_val = min<idx_t>(numeric_limits<T>::max(), count);
			for (idx_t i = 0; i < count; i++) {
				col_ptr[i] = max_val - (((column + 1) * i) % max_val);
			}
		} else if (datagen == "skewed") {
			const T max_val = numeric_limits<T>::max();
			const T unique_vals = 1 << 7;
			const T gap = max_val / unique_vals;
			for (idx_t i = 0; i < count; i++) {
				T r = rand();
				col_ptr[i] = (r % unique_vals) * gap;
			}
		} else if (datagen == "random") {
			const T max_val = numeric_limits<T>::max();
			for (idx_t i = 0; i < count; i++) {
				double r = (double)rand() / RAND_MAX;
				col_ptr[i] = r * max_val;
			}
		} else {
			assert(false);
		}
	}
}

template <class T>
void PrintColumns(vector<unique_ptr<data_t[]>> &columns, const idx_t &count, const idx_t &num_cols) {
	ostringstream output;
	for (idx_t i = 0; i < count; i++) {
		for (idx_t column = 0; column < columns.size(); column++) {
			auto &col = columns[column];
			if (columns.size() > num_cols && column == columns.size() - 1) {
				output << "\t" << *((uint32_t *)col.get() + i);
			} else {
				output << *((T *)col.get() + i) << "\t";
			}
		}
		output << endl;
	}
	cout << output.str() << endl << endl;
}

template <class T>
void PrintRows(data_ptr_t row_ptr, const idx_t &count, const vector<idx_t> &col_widths, const bool &row_id) {
	ostringstream output;
	for (idx_t i = 0; i < count; i++) {
		for (idx_t column = 0; column < col_widths.size(); column++) {
			if (row_id && column == col_widths.size() - 1) {
				output << "\t" << *((uint32_t *)row_ptr);
			} else {
				output << *((T *)row_ptr) << "\t";
			}
			row_ptr += col_widths[column];
		}
		output << endl;
	}
	cout << output.str() << endl << endl;
}

bool IsLittleEndian() {
	int n = 1;
	if (*(char *)&n == 1) {
		return true;
	} else {
		return false;
	}
}

uint8_t FlipSign(uint8_t key_byte) {
	return key_byte ^ 128;
}

template <class T>
void EncodeData(data_ptr_t dataptr, T value, bool is_little_endian) {
	throw exception();
}

template <typename T>
const T Load(const data_ptr_t ptr) {
	T ret;
	memcpy(&ret, ptr, sizeof(ret));
	return ret;
}

template <typename T>
void Store(const T val, data_ptr_t ptr) {
	memcpy(ptr, (void *)&val, sizeof(val));
}

template <>
void EncodeData(data_ptr_t dataptr, int8_t value, bool is_little_endian) {
	Store<uint8_t>(value, dataptr);
	dataptr[0] = FlipSign(dataptr[0]);
}

template <>
void EncodeData(data_ptr_t dataptr, int16_t value, bool is_little_endian) {
	Store<uint16_t>(is_little_endian ? BSWAP16(value) : value, dataptr);
	dataptr[0] = FlipSign(dataptr[0]);
}

template <>
void EncodeData(data_ptr_t dataptr, int32_t value, bool is_little_endian) {
	Store<uint32_t>(is_little_endian ? BSWAP32(value) : value, dataptr);
	dataptr[0] = FlipSign(dataptr[0]);
}

template <>
void EncodeData(data_ptr_t dataptr, int64_t value, bool is_little_endian) {
	Store<uint64_t>(is_little_endian ? BSWAP64(value) : value, dataptr);
	dataptr[0] = FlipSign(dataptr[0]);
}

template <>
void EncodeData(data_ptr_t dataptr, uint8_t value, bool is_little_endian) {
	Store<uint8_t>(value, dataptr);
}

template <>
void EncodeData(data_ptr_t dataptr, uint16_t value, bool is_little_endian) {
	Store<uint16_t>(is_little_endian ? BSWAP16(value) : value, dataptr);
}

template <>
void EncodeData(data_ptr_t dataptr, uint32_t value, bool is_little_endian) {
	Store<uint32_t>(is_little_endian ? BSWAP32(value) : value, dataptr);
}

template <>
void EncodeData(data_ptr_t dataptr, uint64_t value, bool is_little_endian) {
	Store<uint64_t>(is_little_endian ? BSWAP64(value) : value, dataptr);
}

template <class T>
unique_ptr<data_t[]> Scatter(vector<unique_ptr<data_t[]>> columns, const idx_t &count, const idx_t &row_width,
                             const vector<idx_t> &col_widths, const vector<bool> &radix) {
	bool is_little_endian = IsLittleEndian();
	auto result = AllocateRows(count, row_width);
	idx_t i = 0;
	while (i != count) {
		const idx_t next = min<idx_t>(count - i, STANDARD_VECTOR_SIZE);
		idx_t row_offset = 0;
		for (idx_t column = 0; column < columns.size(); column++) {
			const auto &col_width = col_widths[column];
			T *source = (T *)(columns[column].get() + i * col_width);
			data_ptr_t target_ptr = result.get() + i * row_width + row_offset;
			if (radix[column]) {
				for (idx_t j = 0; j < next; j++) {
					EncodeData<T>(target_ptr, source[j], is_little_endian);
					target_ptr += row_width;
				}
			} else {
				for (idx_t j = 0; j < next; j++) {
					Store<T>(source[j], target_ptr);
					target_ptr += row_width;
				}
			}
			row_offset += col_width;
		}
		i += next;
	}
	return result;
}

template <class T>
vector<unique_ptr<data_t[]>> Gather(unique_ptr<data_t[]> rows, const idx_t &count, const idx_t &row_width,
                                    const vector<idx_t> &col_widths) {
	vector<unique_ptr<data_t[]>> result;
	for (idx_t column = 0; column < col_widths.size(); column++) {
		result.push_back(unique_ptr<data_t[]>(new data_t[count * col_widths[column]]));
	}
	idx_t i = 0;
	while (i != count) {
		const idx_t next = min<idx_t>(count - i, STANDARD_VECTOR_SIZE);
		for (idx_t column = 0; column < col_widths.size(); column++) {
			const auto &col_width = col_widths[column];
			data_ptr_t source_ptr = rows.get() + i * row_width + column * col_width;
			T *target = (T *)(result[column].get() + i * col_width);
			for (idx_t j = 0; j < next; j++) {
				target[j] = Load<T>(source_ptr);
				source_ptr += row_width;
			}
		}
		i += next;
	}
	return result;
}

unique_ptr<data_t[]> GatherRowID(unique_ptr<data_t[]> &rows, const idx_t &count, const idx_t &row_width,
                                 const idx_t &offset) {
	auto result = unique_ptr<data_t[]>(new data_t[count * sizeof(uint32_t)]);
	data_ptr_t source_ptr = rows.get() + offset;
	uint32_t *target = (uint32_t *)result.get();
	for (idx_t i = 0; i < count; i++) {
		target[i] = Load<uint32_t>(source_ptr);
		source_ptr += row_width;
	}
	return result;
}

//===--------------------------------------------------------------------===//
// ReOrder Simulation
//===--------------------------------------------------------------------===//
string CreateReOrderCSVHeader() {
	return "category,count,columns,col_width,total,reorder,scatter,gather";
}

template <class T>
vector<unique_ptr<data_t[]>> ReOrderColumns(const uint32_t row_ids[], const vector<unique_ptr<data_t[]>> &columns,
                                            const idx_t &count) {
	auto result = AllocateColumns(count, columns.size(), sizeof(T));
	for (idx_t column = 0; column < columns.size(); column++) {
		T *source = (T *)columns[column].get();
		T *target = (T *)result[column].get();
		for (idx_t i = 0; i < count; i++) {
			target[i] = source[row_ids[i]];
		}
	}
	return result;
}

unique_ptr<data_t[]> ReOrderRows(const uint32_t row_ids[], unique_ptr<data_t[]> rows, const idx_t &count,
                                 const idx_t &row_width) {
	auto result = AllocateRows(count, row_width);
	auto source_ptr = rows.get();
	auto target_ptr = result.get();
	for (idx_t i = 0; i < count; i++) {
		duckdb::fast_memcpy(target_ptr, source_ptr + row_ids[i] * row_width, row_width);
		target_ptr += row_width;
	}
	return result;
}

template <class T>
string SimulateColumnReOrder(const uint32_t row_ids[], const idx_t &count, const idx_t &columns) {
	// Initialize source data
	auto source = AllocateColumns(count, columns, sizeof(T));

	// ReOrder and timestamp
	auto before_timestamp = CurrentTime();
#ifdef TRACE
	this_thread::sleep_for(seconds(2));
#endif
	auto target = ReOrderColumns<T>(row_ids, source, count);
#ifdef TRACE
	this_thread::sleep_for(seconds(2));
#endif
	auto after_timestamp = CurrentTime();

	// Compute duration of phases
	auto total_duration = after_timestamp - before_timestamp;
	return CreateOutput("col", {count, columns, sizeof(T), total_duration, total_duration}, 8);
}

template <class T>
string SimulateRowReOrder(const T row_ids[], const idx_t &count, const idx_t &columns) {
	// Initialize source data
	auto source = AllocateColumns(count, columns, sizeof(T));

	// Scatter, ReOrder, Gather and timestamp
	idx_t row_width = 0;
	vector<idx_t> col_widths;
	vector<bool> radix;
	for (idx_t i = 0; i < columns; i++) {
		row_width += sizeof(T);
		col_widths.push_back(sizeof(T));
		radix.push_back(false);
	}

#ifdef TRACE
	this_thread::sleep_for(seconds(2));
#endif
	auto before_timestamp = CurrentTime();
	auto source_rows = Scatter<T>(move(source), count, row_width, col_widths, radix);
	auto scatter_timestamp = CurrentTime();

	auto target_rows = ReOrderRows(row_ids, move(source_rows), count, row_width);
	auto reorder_timestamp = CurrentTime();

	auto target = Gather<T>(move(target_rows), count, row_width, col_widths);
	auto after_timestamp = CurrentTime();
#ifdef TRACE
	this_thread::sleep_for(seconds(2));
#endif

	// Compute duration of phases
	auto total_duration = after_timestamp - before_timestamp;
	auto scatter_duration = scatter_timestamp - before_timestamp;
	auto reorder_duration = reorder_timestamp - scatter_timestamp;
	auto gather_duration = after_timestamp - reorder_timestamp;
	return CreateOutput(
	    "row", {count, columns, sizeof(T), total_duration, reorder_duration, scatter_duration, gather_duration}, 8);
}

template <class T>
string SimulateReOrder(idx_t count, idx_t columns) {
	auto row_ids = InitRowIDs(count, true);
	ostringstream result;
	result << SimulateColumnReOrder<T>((uint32_t *)row_ids.get(), count, columns) << endl;
	result << SimulateRowReOrder<T>((uint32_t *)row_ids.get(), count, columns) << endl;
	return result.str();
}

template <class T>
void SimulateReOrder(idx_t row_max, idx_t col_max, idx_t iterations) {
	cout << "SimulateReOrder" << endl;
	ofstream results_file("results/reorder.csv", ios::trunc);
	results_file << CreateReOrderCSVHeader() << endl;
	for (idx_t r = 10; r < row_max; r += 2) {
		for (idx_t c = 0; c < col_max; c++) {
			for (idx_t i = 0; i < iterations; i++) {
				idx_t num_cols = min<idx_t>(1 << c, 96);
				results_file << SimulateReOrder<T>(1 << r, num_cols);
				results_file.flush();
				cout << "." << flush;
			}
		}
		cout << endl;
	}
}

template <class T>
void VerifyRowReOrder(const uint32_t row_ids[], idx_t count, idx_t columns) {
	// Initialize source data
	auto source = AllocateColumns(count, columns, sizeof(T));
	FillColumns<T>(source, count, "deterministic");
	cout << "--- BEFORE --- " << endl;
	PrintColumns<T>(source, count, columns);

	// Scatter, ReOrder, Gather, and print
	idx_t row_width = 0;
	vector<idx_t> col_widths;
	vector<bool> radix;
	for (idx_t i = 0; i < columns; i++) {
		row_width += sizeof(T);
		col_widths.push_back(sizeof(T));
		radix.push_back(false);
	}

	auto source_rows = Scatter<T>(move(source), count, row_width, col_widths, radix);
	cout << "--- SCATTERED --- " << endl;
	PrintRows<T>(source_rows.get(), count, col_widths, false);

	auto target_rows = ReOrderRows(row_ids, move(source_rows), count, row_width);
	cout << "--- REORDERED --- " << endl;
	PrintRows<T>(target_rows.get(), count, col_widths, false);

	auto target = Gather<T>(move(target_rows), count, row_width, col_widths);
	cout << "--- GATHERED --- " << endl;
	PrintColumns<T>(target, count, columns);
}

template <class T>
void VerifyColumnReOrder(const uint32_t row_ids[], idx_t count, idx_t columns) {
	// Initialize source data
	auto source = AllocateColumns(count, columns, sizeof(T));
	FillColumns<T>(source, count, "deterministic");
	cout << "--- BEFORE --- " << endl;
	PrintColumns<T>(source, count, columns);

	// ReOrder and print
	auto target = ReOrderColumns<T>(row_ids, source, count);
	cout << "--- AFTER --- " << endl;
	PrintColumns<T>(target, count, columns);
}

void VerifyReOrder() {
	idx_t count = 5;
	idx_t columns = 5;

	auto row_ids = InitRowIDs(count, true);
	VerifyRowReOrder<idx_t>((uint32_t *)row_ids.get(), count, columns);
	VerifyColumnReOrder<idx_t>((uint32_t *)row_ids.get(), count, columns);
}

//===--------------------------------------------------------------------===//
// Comparator Simulation
//===--------------------------------------------------------------------===//
string CreateComparatorCSVHeader() {
	return "category,count,columns,col_width,total,sort,scatter,gather";
}

template <class T>
struct NormalizedRowOrderEntry1 {
public:
	T col1;
	uint32_t row_id;

public:
	inline bool operator<(const NormalizedRowOrderEntry1 &rhs) const {
		return memcmp(&col1, &rhs.col1, sizeof(T)) < 0;
	}
};

template <class T>
struct NormalizedRowOrderEntry2 {
public:
	T col1;
	T col2;
	uint32_t row_id;

public:
	inline bool operator<(const NormalizedRowOrderEntry2 &rhs) const {
		return memcmp(&col1, &rhs.col1, 2 * sizeof(T)) < 0;
	}
};

template <class T>
struct NormalizedRowOrderEntry3 {
public:
	T col1;
	T col2;
	T col3;
	uint32_t row_id;

public:
	inline bool operator<(const NormalizedRowOrderEntry3 &rhs) const {
		return memcmp(&col1, &rhs.col1, 3 * sizeof(T)) < 0;
	}
};

template <class T>
struct NormalizedRowOrderEntry4 {
public:
	T col1;
	T col2;
	T col3;
	T col4;
	uint32_t row_id;

public:
	inline bool operator<(const NormalizedRowOrderEntry4 &rhs) const {
		return memcmp(&col1, &rhs.col1, 4 * sizeof(T)) < 0;
	}
};

template <class T>
struct NormalizedRowOrderEntry5 {
public:
	T col1;
	T col2;
	T col3;
	T col4;
	T col5;
	uint32_t row_id;

public:
	inline bool operator<(const NormalizedRowOrderEntry5 &rhs) const {
		return memcmp(&col1, &rhs.col1, 5 * sizeof(T)) < 0;
	}
};

template <class T>
struct NormalizedRowOrderEntry6 {
public:
	T col1;
	T col2;
	T col3;
	T col4;
	T col5;
	T col6;
	uint32_t row_id;

public:
	inline bool operator<(const NormalizedRowOrderEntry6 &rhs) const {
		return memcmp(&col1, &rhs.col1, 6 * sizeof(T)) < 0;
	}
};

template <class T>
struct NormalizedRowOrderEntry7 {
public:
	T col1;
	T col2;
	T col3;
	T col4;
	T col5;
	T col6;
	T col7;
	uint32_t row_id;

public:
	inline bool operator<(const NormalizedRowOrderEntry7 &rhs) const {
		return memcmp(&col1, &rhs.col1, 7 * sizeof(T)) < 0;
	}
};

template <class T>
struct NormalizedRowOrderEntry8 {
public:
	T col1;
	T col2;
	T col3;
	T col4;
	T col5;
	T col6;
	T col7;
	T col8;
	uint32_t row_id;

public:
	inline bool operator<(const NormalizedRowOrderEntry8 &rhs) const {
		return memcmp(&col1, &rhs.col1, 8 * sizeof(T)) < 0;
	}
};

template <class T>
struct BranchedRowOrderEntry1 {
public:
	T col1;
	uint32_t row_id;

public:
	inline bool operator<(const BranchedRowOrderEntry1 &rhs) const {
		return col1 < rhs.col1;
	}
};

template <class T>
struct BranchedRowOrderEntry2 {
public:
	T col1;
	T col2;
	uint32_t row_id;

public:
	inline bool operator<(const BranchedRowOrderEntry2 &rhs) const {
		const T *order_columns = &col1;
		const T *rhs_order_columns = &rhs.col1;
		idx_t i;
		for (i = 0; i < 1; i++) {
			if (order_columns[i] != rhs_order_columns[i]) {
				break;
			}
		}
		return order_columns[i] < rhs_order_columns[i];
	}
};

template <class T>
struct BranchedRowOrderEntry3 {
public:
	T col1;
	T col2;
	T col3;
	uint32_t row_id;

public:
	inline bool operator<(const BranchedRowOrderEntry3 &rhs) const {
		const T *order_columns = &col1;
		const T *rhs_order_columns = &rhs.col1;
		idx_t i;
		for (i = 0; i < 2; i++) {
			if (order_columns[i] != rhs_order_columns[i]) {
				break;
			}
		}
		return order_columns[i] < rhs_order_columns[i];
	}
};

template <class T>
struct BranchedRowOrderEntry4 {
public:
	T col1;
	T col2;
	T col3;
	T col4;
	uint32_t row_id;

public:
	inline bool operator<(const BranchedRowOrderEntry4 &rhs) const {
		const T *order_columns = &col1;
		const T *rhs_order_columns = &rhs.col1;
		idx_t i;
		for (i = 0; i < 3; i++) {
			if (order_columns[i] != rhs_order_columns[i]) {
				break;
			}
		}
		return order_columns[i] < rhs_order_columns[i];
	}
};

template <class T>
struct BranchedRowOrderEntry5 {
public:
	T col1;
	T col2;
	T col3;
	T col4;
	T col5;
	uint32_t row_id;

public:
	inline bool operator<(const BranchedRowOrderEntry5 &rhs) const {
		const T *order_columns = &col1;
		const T *rhs_order_columns = &rhs.col1;
		idx_t i;
		for (i = 0; i < 4; i++) {
			if (order_columns[i] != rhs_order_columns[i]) {
				break;
			}
		}
		return order_columns[i] < rhs_order_columns[i];
	}
};

template <class T>
struct BranchedRowOrderEntry6 {
public:
	T col1;
	T col2;
	T col3;
	T col4;
	T col5;
	T col6;
	uint32_t row_id;

public:
	inline bool operator<(const BranchedRowOrderEntry6 &rhs) const {
		const T *order_columns = &col1;
		const T *rhs_order_columns = &rhs.col1;
		idx_t i;
		for (i = 0; i < 5; i++) {
			if (order_columns[i] != rhs_order_columns[i]) {
				break;
			}
		}
		return order_columns[i] < rhs_order_columns[i];
	}
};

template <class T>
struct BranchedRowOrderEntry7 {
public:
	T col1;
	T col2;
	T col3;
	T col4;
	T col5;
	T col6;
	T col7;
	uint32_t row_id;

public:
	inline bool operator<(const BranchedRowOrderEntry7 &rhs) const {
		const T *order_columns = &col1;
		const T *rhs_order_columns = &rhs.col1;
		idx_t i;
		for (i = 0; i < 6; i++) {
			if (order_columns[i] != rhs_order_columns[i]) {
				break;
			}
		}
		return order_columns[i] < rhs_order_columns[i];
	}
};

template <class T>
struct BranchedRowOrderEntry8 {
public:
	T col1;
	T col2;
	T col3;
	T col4;
	T col5;
	T col6;
	T col7;
	T col8;
	uint32_t row_id;

public:
	inline bool operator<(const BranchedRowOrderEntry8 &rhs) const {
		const T *order_columns = &col1;
		const T *rhs_order_columns = &rhs.col1;
		idx_t i;
		for (i = 0; i < 7; i++) {
			if (order_columns[i] != rhs_order_columns[i]) {
				break;
			}
		}
		return order_columns[i] < rhs_order_columns[i];
	}
};

template <class T>
struct BranchlessRowOrderEntry1 {
public:
	T col1;
	uint32_t row_id;

public:
	inline auto operator<=>(const BranchlessRowOrderEntry1 &) const = default;

	inline bool operator<(const BranchlessRowOrderEntry1 &rhs) const {
		return col1 < rhs.col1;
	}
};

template <class T>
struct BranchlessRowOrderEntry2 {
public:
	T col1;
	T col2;
	uint32_t row_id;

public:
	inline auto operator<=>(const BranchlessRowOrderEntry2 &) const = default;

	inline bool operator<(const BranchlessRowOrderEntry2 &rhs) const {
		const T *l = &col1;
		const T *r = &rhs.col1;
		int8_t comp = 0;
		for (idx_t i = 0; i < 2; i++) {
			auto next_comp = l[i] <=> r[i];
			int8_t se = comp == 0;
			se |= (se << 7);
			comp |= *((int8_t *)&next_comp) & se;
		}
		return comp < 0;
	}
};

template <class T>
struct BranchlessRowOrderEntry3 {
public:
	T col1;
	T col2;
	T col3;
	uint32_t row_id;

public:
	inline auto operator<=>(const BranchlessRowOrderEntry3 &) const = default;

	inline bool operator<(const BranchlessRowOrderEntry3 &rhs) const {
		const T *l = &col1;
		const T *r = &rhs.col1;
		int8_t comp = 0;
		for (idx_t i = 0; i < 3; i++) {
			auto next_comp = l[i] <=> r[i];
			int8_t se = comp == 0;
			se |= (se << 7);
			comp |= *((int8_t *)&next_comp) & se;
		}
		return comp < 0;
	}
};

template <class T>
struct BranchlessRowOrderEntry4 {
public:
	T col1;
	T col2;
	T col3;
	T col4;
	uint32_t row_id;

public:
	inline auto operator<=>(const BranchlessRowOrderEntry4 &) const = default;

	inline bool operator<(const BranchlessRowOrderEntry4 &rhs) const {
		const T *l = &col1;
		const T *r = &rhs.col1;
		int8_t comp = 0;
		for (idx_t i = 0; i < 4; i++) {
			auto next_comp = l[i] <=> r[i];
			int8_t se = comp == 0;
			se |= (se << 7);
			comp |= *((int8_t *)&next_comp) & se;
		}
		return comp < 0;
	}
};

template <class T>
struct BranchlessRowOrderEntry5 {
public:
	T col1;
	T col2;
	T col3;
	T col4;
	T col5;
	uint32_t row_id;

public:
	inline auto operator<=>(const BranchlessRowOrderEntry5 &) const = default;

	inline bool operator<(const BranchlessRowOrderEntry5 &rhs) const {
		const T *l = &col1;
		const T *r = &rhs.col1;
		int8_t comp = 0;
		for (idx_t i = 0; i < 5; i++) {
			auto next_comp = l[i] <=> r[i];
			int8_t se = comp == 0;
			se |= (se << 7);
			comp |= *((int8_t *)&next_comp) & se;
		}
		return comp < 0;
	}
};

template <class T>
struct BranchlessRowOrderEntry6 {
public:
	T col1;
	T col2;
	T col3;
	T col4;
	T col5;
	T col6;
	uint32_t row_id;

public:
	inline auto operator<=>(const BranchlessRowOrderEntry6 &) const = default;

	inline bool operator<(const BranchlessRowOrderEntry6 &rhs) const {
		const T *l = &col1;
		const T *r = &rhs.col1;
		int8_t comp = 0;
		for (idx_t i = 0; i < 6; i++) {
			auto next_comp = l[i] <=> r[i];
			int8_t se = comp == 0;
			se |= (se << 7);
			comp |= *((int8_t *)&next_comp) & se;
		}
		return comp < 0;
	}
};

template <class T>
struct BranchlessRowOrderEntry7 {
public:
	T col1;
	T col2;
	T col3;
	T col4;
	T col5;
	T col6;
	T col7;
	uint32_t row_id;

public:
	inline auto operator<=>(const BranchlessRowOrderEntry7 &) const = default;

	inline bool operator<(const BranchlessRowOrderEntry7 &rhs) const {
		const T *l = &col1;
		const T *r = &rhs.col1;
		int8_t comp = 0;
		for (idx_t i = 0; i < 7; i++) {
			auto next_comp = l[i] <=> r[i];
			int8_t se = comp == 0;
			se |= (se << 7);
			comp |= *((int8_t *)&next_comp) & se;
		}
		return comp < 0;
	}
};

template <class T>
struct BranchlessRowOrderEntry8 {
public:
	T col1;
	T col2;
	T col3;
	T col4;
	T col5;
	T col6;
	T col7;
	T col8;
	uint32_t row_id;

public:
	inline auto operator<=>(const BranchlessRowOrderEntry8 &) const = default;

	inline bool operator<(const BranchlessRowOrderEntry8 &rhs) const {
		const T *l = &col1;
		const T *r = &rhs.col1;
		int8_t comp = 0;
		for (idx_t i = 0; i < 8; i++) {
			auto next_comp = l[i] <=> r[i];
			int8_t se = comp == 0;
			se |= (se << 7);
			comp |= *((int8_t *)&next_comp) & se;
		}
		return comp < 0;
	}
};

template <class T>
struct NormalizedKeyComparator {
public:
	NormalizedKeyComparator(const idx_t &comp_width) : comp_width(comp_width) {
	}

	inline bool operator()(const T &lhs, const T &rhs) const {
		return memcmp(Ptr(lhs), Ptr(rhs), comp_width) < 0;
	}

	inline data_ptr_t Ptr(const T &a) const {
		return (data_ptr_t)&a;
	}

private:
	const idx_t comp_width;
};

template <class T>
void SortRowNormalized(data_ptr_t row_data, const idx_t &count, const idx_t &columns, string method) {
	auto comp = NormalizedKeyComparator<T>(columns * sizeof(T));
	switch (columns) {
	case 1: {
		auto row_data_ptr = (NormalizedRowOrderEntry1<T> *)row_data;
		if (method == "pdq_static") {
			pdqsort_branchless(row_data_ptr, row_data_ptr + count);
		} else {
			auto comp = NormalizedKeyComparator<NormalizedRowOrderEntry1<T>>(columns * sizeof(T));
			pdqsort_branchless(row_data_ptr, row_data_ptr + count, comp);
		}
		break;
	}
	case 2: {
		auto row_data_ptr = (NormalizedRowOrderEntry2<T> *)row_data;
		if (method == "pdq_static") {
			pdqsort_branchless(row_data_ptr, row_data_ptr + count);
		} else {
			auto comp = NormalizedKeyComparator<NormalizedRowOrderEntry2<T>>(columns * sizeof(T));
			pdqsort_branchless(row_data_ptr, row_data_ptr + count, comp);
		}
		break;
	}
	case 3: {
		auto row_data_ptr = (NormalizedRowOrderEntry3<T> *)row_data;
		if (method == "pdq_static") {
			pdqsort_branchless(row_data_ptr, row_data_ptr + count);
		} else {
			auto comp = NormalizedKeyComparator<NormalizedRowOrderEntry3<T>>(columns * sizeof(T));
			pdqsort_branchless(row_data_ptr, row_data_ptr + count, comp);
		}
		break;
	}
	case 4: {
		auto row_data_ptr = (NormalizedRowOrderEntry4<T> *)row_data;
		if (method == "pdq_static") {
			pdqsort_branchless(row_data_ptr, row_data_ptr + count);
		} else {
			auto comp = NormalizedKeyComparator<NormalizedRowOrderEntry4<T>>(columns * sizeof(T));
			pdqsort_branchless(row_data_ptr, row_data_ptr + count, comp);
		}
		break;
	}
	case 5: {
		auto row_data_ptr = (NormalizedRowOrderEntry5<T> *)row_data;
		if (method == "pdq_static") {
			pdqsort_branchless(row_data_ptr, row_data_ptr + count);
		} else {
			auto comp = NormalizedKeyComparator<NormalizedRowOrderEntry5<T>>(columns * sizeof(T));
			pdqsort_branchless(row_data_ptr, row_data_ptr + count, comp);
		}
		break;
	}
	case 6: {
		auto row_data_ptr = (NormalizedRowOrderEntry6<T> *)row_data;
		if (method == "pdq_static") {
			pdqsort_branchless(row_data_ptr, row_data_ptr + count);
		} else {
			auto comp = NormalizedKeyComparator<NormalizedRowOrderEntry6<T>>(columns * sizeof(T));
			pdqsort_branchless(row_data_ptr, row_data_ptr + count, comp);
		}
		break;
	}
	case 7: {
		auto row_data_ptr = (NormalizedRowOrderEntry7<T> *)row_data;
		if (method == "pdq_static") {
			pdqsort_branchless(row_data_ptr, row_data_ptr + count);
		} else {
			auto comp = NormalizedKeyComparator<NormalizedRowOrderEntry7<T>>(columns * sizeof(T));
			pdqsort_branchless(row_data_ptr, row_data_ptr + count, comp);
		}
		break;
	}
	case 8: {
		auto row_data_ptr = (NormalizedRowOrderEntry8<T> *)row_data;
		if (method == "pdq_static") {
			pdqsort_branchless(row_data_ptr, row_data_ptr + count);
		} else {
			auto comp = NormalizedKeyComparator<NormalizedRowOrderEntry8<T>>(columns * sizeof(T));
			pdqsort_branchless(row_data_ptr, row_data_ptr + count, comp);
		}
		break;
	}
	default:
		throw exception();
	}
}

template <class T>
void SortRowBranched(data_ptr_t row_data, const idx_t &count, const idx_t &columns) {
	switch (columns) {
	case 1: {
		auto row_data_ptr = (BranchedRowOrderEntry1<T> *)row_data;
		pdqsort_branchless(row_data_ptr, row_data_ptr + count);
		break;
	}
	case 2: {
		auto row_data_ptr = (BranchedRowOrderEntry2<T> *)row_data;
		pdqsort_branchless(row_data_ptr, row_data_ptr + count);
		break;
	}
	case 3: {
		auto row_data_ptr = (BranchedRowOrderEntry3<T> *)row_data;
		pdqsort_branchless(row_data_ptr, row_data_ptr + count);
		break;
	}
	case 4: {
		auto row_data_ptr = (BranchedRowOrderEntry4<T> *)row_data;
		pdqsort_branchless(row_data_ptr, row_data_ptr + count);
		break;
	}
	case 5: {
		auto row_data_ptr = (BranchedRowOrderEntry5<T> *)row_data;
		pdqsort_branchless(row_data_ptr, row_data_ptr + count);
		break;
	}
	case 6: {
		auto row_data_ptr = (BranchedRowOrderEntry6<T> *)row_data;
		pdqsort_branchless(row_data_ptr, row_data_ptr + count);
		break;
	}
	case 7: {
		auto row_data_ptr = (BranchedRowOrderEntry7<T> *)row_data;
		pdqsort_branchless(row_data_ptr, row_data_ptr + count);
		break;
	}
	case 8: {
		auto row_data_ptr = (BranchedRowOrderEntry8<T> *)row_data;
		pdqsort_branchless(row_data_ptr, row_data_ptr + count);
		break;
	}
	default:
		assert(false);
	}
}

template <class T>
void SortRowBranchless(data_ptr_t row_data, const idx_t &count, const idx_t &columns) {
	switch (columns) {
	case 1: {
		auto row_data_ptr = (BranchlessRowOrderEntry1<T> *)row_data;
		pdqsort_branchless(row_data_ptr, row_data_ptr + count);
		break;
	}
	case 2: {
		auto row_data_ptr = (BranchlessRowOrderEntry2<T> *)row_data;
		pdqsort_branchless(row_data_ptr, row_data_ptr + count);
		break;
	}
	case 3: {
		auto row_data_ptr = (BranchlessRowOrderEntry3<T> *)row_data;
		pdqsort_branchless(row_data_ptr, row_data_ptr + count);
		break;
	}
	case 4: {
		auto row_data_ptr = (BranchlessRowOrderEntry4<T> *)row_data;
		pdqsort_branchless(row_data_ptr, row_data_ptr + count);
		break;
	}
	case 5: {
		auto row_data_ptr = (BranchlessRowOrderEntry5<T> *)row_data;
		pdqsort_branchless(row_data_ptr, row_data_ptr + count);
		break;
	}
	case 6: {
		auto row_data_ptr = (BranchlessRowOrderEntry6<T> *)row_data;
		pdqsort_branchless(row_data_ptr, row_data_ptr + count);
		break;
	}
	case 7: {
		auto row_data_ptr = (BranchlessRowOrderEntry7<T> *)row_data;
		pdqsort_branchless(row_data_ptr, row_data_ptr + count);
		break;
	}
	case 8: {
		auto row_data_ptr = (BranchlessRowOrderEntry8<T> *)row_data;
		pdqsort_branchless(row_data_ptr, row_data_ptr + count);
		break;
	}
	default:
		throw exception();
	}
}

template <class T>
inline static bool CompareColumnarBranch(const vector<const T *> &l_cols, const vector<const T *> &r_cols,
                                         const idx_t &l_i, const idx_t &r_i, const idx_t &columns) {
	idx_t col_idx;
	for (col_idx = 0; col_idx < columns - 1; col_idx++) {
		if (l_cols[col_idx][l_i] != r_cols[col_idx][r_i]) {
			break;
		}
	}
	return l_cols[col_idx][l_i] < r_cols[col_idx][r_i];
}

template <class T>
inline static bool CompareColumnarBranchless(const vector<const T *> &l_cols, const vector<const T *> &r_cols,
                                             const idx_t &l_i, const idx_t &r_i, const idx_t &columns) {
	int8_t comp = 0;
	for (idx_t col_idx = 0; col_idx < columns; col_idx++) {
		auto next_comp = l_cols[col_idx][l_i] <=> r_cols[col_idx][r_i];
		int8_t se = comp == 0;
		se |= (se << 7);
		comp |= *((int8_t *)&next_comp) & se;
	}
	return comp < 0;
}

template <class T>
void SortColumn(unique_ptr<data_t[]> &row_id_col, vector<unique_ptr<data_t[]>> &columns, const idx_t &count,
                string method) {
	uint32_t *row_ids = (uint32_t *)row_id_col.get();
	vector<const T *> cols;
	for (const auto &col : columns) {
		cols.push_back((const T *)col.get());
	}
	const idx_t num_cols = cols.size();
	if (num_cols == 1) {
		const auto &col = cols[0];
		pdqsort_branchless(
		    row_ids, row_ids + count,
		    [&row_ids, &col](const uint32_t &lhs, const uint32_t &rhs) -> bool { return col[lhs] < col[rhs]; });
	} else if (method == "col" || method == "col_all") {
		pdqsort_branchless(row_ids, row_ids + count,
		                   [&cols, &num_cols](const uint32_t &lhs, const uint32_t &rhs) -> bool {
			                   return CompareColumnarBranch<T>(cols, cols, lhs, rhs, num_cols);
		                   });
	} else if (method == "col_branchless") {
		pdqsort_branchless(row_ids, row_ids + count,
		                   [&cols, &num_cols](const uint32_t &lhs, const uint32_t &rhs) -> bool {
			                   return CompareColumnarBranchless<T>(cols, cols, lhs, rhs, num_cols);
		                   });
	} else {
		assert(false);
	}
}

template <class T>
bool ComputeColumnTies(bool ties[], uint32_t row_ids[], const T col[], const idx_t &count) {
	bool any_tie = false;
	for (idx_t i = 0; i < count - 1; i++) {
		ties[i] = ties[i] && col[row_ids[i]] == col[row_ids[i + 1]];
		any_tie = any_tie || ties[i];
	}
	return any_tie;
}

template <class T>
void SortColumnSubsort(unique_ptr<data_t[]> &row_id_col, vector<unique_ptr<data_t[]>> &columns, const idx_t &count) {
	uint32_t *row_ids = (uint32_t *)row_id_col.get();
	vector<T *> cols;
	for (const auto &col : columns) {
		cols.push_back((T *)col.get());
	}
	if (cols.size() == 1) {
		const auto &col = cols[0];
		pdqsort_branchless(
		    row_ids, row_ids + count,
		    [&row_ids, &col](const uint32_t &lhs, const uint32_t &rhs) -> bool { return col[lhs] < col[rhs]; });
		return;
	}
	unique_ptr<bool[]> ties_ptr;
	bool *ties = nullptr;
	for (idx_t col_idx = 0; col_idx < cols.size(); col_idx++) {
		const auto &col = cols[col_idx];
		if (!ties) {
			// This is the first sort
			pdqsort_branchless(
			    row_ids, row_ids + count,
			    [&row_ids, &col](const uint32_t &lhs, const uint32_t &rhs) -> bool { return col[lhs] < col[rhs]; });
			// Initialize ties array
			ties_ptr = unique_ptr<bool[]>(new bool[count]);
			ties = ties_ptr.get();
			std::fill_n(ties, count - 1, true);
			ties[count - 1] = false;
		} else {
			if (!ComputeColumnTies(ties, row_ids, cols[col_idx - 1], count)) {
				break;
			}
			// Subsort tied tuples
			for (idx_t i = 0; i < count; i++) {
				if (!ties[i]) {
					continue;
				}
				idx_t j;
				for (j = i + 1; j < count; j++) {
					if (!ties[j]) {
						break;
					}
				}
				pdqsort_branchless(
				    row_ids + i, row_ids + j + 1,
				    [&row_ids, &col](const uint32_t &lhs, const uint32_t &rhs) -> bool { return col[lhs] < col[rhs]; });
				i = j;
			}
		}
	}
}

template <class ROW, class T>
bool ComputeRowTies(bool ties[], ROW *row_data_ptr, const idx_t &count, const idx_t &col_idx) {
	assert(sizeof(ROW) % sizeof(T) == 0);
	const idx_t width = sizeof(ROW) / sizeof(T);
	T *col_ptr = (T *)row_data_ptr + col_idx;

	bool any_tie = false;
	for (idx_t i = 0; i < count - 1; i++) {
		T &curr = *col_ptr;
		col_ptr += width;
		ties[i] = ties[i] && curr == *col_ptr;
		any_tie = any_tie || ties[i];
	}
	return any_tie;
}

template <class ROW, class T>
void SortRowSubsortPDQ(ROW *row_data_ptr, const idx_t &count, const idx_t &col_idx) {
	switch (col_idx) {
	case 0:
		return pdqsort_branchless(row_data_ptr, row_data_ptr + count,
		                          [](const ROW &lhs, const ROW &rhs) -> bool { return lhs.col1 < rhs.col1; });
	case 1:
		return pdqsort_branchless(row_data_ptr, row_data_ptr + count, [](const ROW &lhs, const ROW &rhs) -> bool {
			return *(&lhs.col1 + 1) < *(&rhs.col1 + 1);
		});
	case 2:
		return pdqsort_branchless(row_data_ptr, row_data_ptr + count, [](const ROW &lhs, const ROW &rhs) -> bool {
			return *(&lhs.col1 + 2) < *(&rhs.col1 + 2);
		});
	case 3:
		return pdqsort_branchless(row_data_ptr, row_data_ptr + count, [](const ROW &lhs, const ROW &rhs) -> bool {
			return *(&lhs.col1 + 3) < *(&rhs.col1 + 3);
		});
	case 4:
		return pdqsort_branchless(row_data_ptr, row_data_ptr + count, [](const ROW &lhs, const ROW &rhs) -> bool {
			return *(&lhs.col1 + 4) < *(&rhs.col1 + 4);
		});
	case 5:
		return pdqsort_branchless(row_data_ptr, row_data_ptr + count, [](const ROW &lhs, const ROW &rhs) -> bool {
			return *(&lhs.col1 + 5) < *(&rhs.col1 + 5);
		});
	case 6:
		return pdqsort_branchless(row_data_ptr, row_data_ptr + count, [](const ROW &lhs, const ROW &rhs) -> bool {
			return *(&lhs.col1 + 6) < *(&rhs.col1 + 6);
		});
	case 7:
		return pdqsort_branchless(row_data_ptr, row_data_ptr + count, [](const ROW &lhs, const ROW &rhs) -> bool {
			return *(&lhs.col1 + 7) < *(&rhs.col1 + 7);
		});
	default:
		assert(false);
	}
}

template <class ROW, class T>
void SortRowSubsort(ROW *row_data_ptr, const idx_t &count, const idx_t &columns) {
	if (columns == 1) {
		return SortRowSubsortPDQ<ROW, T>(row_data_ptr, count, 0);
	}
	unique_ptr<bool[]> ties_ptr;
	bool *ties = nullptr;
	for (idx_t col_idx = 0; col_idx < columns; col_idx++) {
		if (!ties) {
			// This is the first sort
			SortRowSubsortPDQ<ROW, T>(row_data_ptr, count, col_idx);
			// Initialize ties array
			ties_ptr = unique_ptr<bool[]>(new bool[count]);
			ties = ties_ptr.get();
			std::fill_n(ties, count - 1, true);
			ties[count - 1] = false;
		} else {
			if (!ComputeRowTies<ROW, T>(ties, row_data_ptr, count, col_idx - 1)) {
				break;
			}
			// Subsort tied tuples
			for (idx_t i = 0; i < count; i++) {
				if (!ties[i]) {
					continue;
				}
				idx_t j;
				for (j = i + 1; j < count; j++) {
					if (!ties[j]) {
						break;
					}
				}
				SortRowSubsortPDQ<ROW, T>(row_data_ptr + i, j - i + 1, col_idx);
				i = j;
			}
		}
	}
}

template <class T>
void SortRowSubsort(data_ptr_t row_data, const idx_t &count, const idx_t &columns) {
	switch (columns) {
	case 1:
		return SortRowSubsort<BranchedRowOrderEntry1<T>, T>((BranchedRowOrderEntry1<T> *)row_data, count, columns);
	case 2:
		return SortRowSubsort<BranchedRowOrderEntry2<T>, T>((BranchedRowOrderEntry2<T> *)row_data, count, columns);
	case 3:
		return SortRowSubsort<BranchedRowOrderEntry3<T>, T>((BranchedRowOrderEntry3<T> *)row_data, count, columns);
	case 4:
		return SortRowSubsort<BranchedRowOrderEntry4<T>, T>((BranchedRowOrderEntry4<T> *)row_data, count, columns);
	case 5:
		return SortRowSubsort<BranchedRowOrderEntry5<T>, T>((BranchedRowOrderEntry5<T> *)row_data, count, columns);
	case 6:
		return SortRowSubsort<BranchedRowOrderEntry6<T>, T>((BranchedRowOrderEntry6<T> *)row_data, count, columns);
	case 7:
		return SortRowSubsort<BranchedRowOrderEntry7<T>, T>((BranchedRowOrderEntry7<T> *)row_data, count, columns);
	case 8:
		return SortRowSubsort<BranchedRowOrderEntry8<T>, T>((BranchedRowOrderEntry8<T> *)row_data, count, columns);
	default:
		assert(false);
	}
}

template <class T>
string SimulateRowComparator(const idx_t &count, const idx_t &columns, string category) {
	// This must hold for for alignment
	assert((columns * sizeof(T)) % sizeof(uint32_t) == 0);

	// Initialize source data
	auto row_ids = InitRowIDs(count, false);
	auto source = AllocateColumns(count, columns, sizeof(T));
	FillColumns<T>(source, count, "skewed");
	source.push_back(move(row_ids));
	// PrintColumns<T>(source, count, columns);

	auto before_timestamp = CurrentTime();
	idx_t row_width = 0;
	vector<idx_t> col_widths;
	vector<bool> radix;
	for (idx_t i = 0; i < columns; i++) {
		row_width += sizeof(T);
		col_widths.push_back(sizeof(T));
		radix.push_back(category == "row_norm");
	}
	row_width += sizeof(uint32_t);
	col_widths.push_back(sizeof(uint32_t));
	radix.push_back(false);

	auto row_data = Scatter<T>(move(source), count, row_width, col_widths, radix);
	auto scatter_timestamp = CurrentTime();
	// PrintRows<T>(row_data.get(), count, col_widths, true);
	// cout << endl;

#ifdef TRACE
	this_thread::sleep_for(seconds(2));
#endif
	if (category == "row_norm") {
		SortRowNormalized<T>(row_data.get(), count, columns, "pdq_static");
	} else if (category == "row_all") {
		SortRowBranched<T>(row_data.get(), count, columns);
	} else if (category == "row_iter") {
		SortRowSubsort<T>(row_data.get(), count, columns);
	} else if (category == "row_all_branchless") {
		SortRowBranchless<T>(row_data.get(), count, columns);
	} else {
		assert(false);
	}
	auto sort_timestamp = CurrentTime();
#ifdef TRACE
	this_thread::sleep_for(seconds(2));
#endif
	// PrintRows<T>(row_data.get(), count, col_widths, true);

	auto target = GatherRowID(row_data, count, row_width, row_width - sizeof(uint32_t));
	auto after_timestamp = CurrentTime();

	// Verification stuff
	// vector<unique_ptr<data_t[]>> dummy_cols;
	// dummy_cols.push_back(move(target));
	// PrintColumns<T>(dummy_cols, count, 0);

	// Compute duration of phases
	auto total_duration = after_timestamp - before_timestamp;
	auto scatter_duration = scatter_timestamp - before_timestamp;
	auto sort_duration = sort_timestamp - scatter_timestamp;
	auto gather_duration = after_timestamp - sort_timestamp;
	return CreateOutput(
	    category, {count, columns, sizeof(T), total_duration, sort_duration, scatter_duration, gather_duration}, 8);
}

template <class T>
void AssertSortedColumns(const data_t idxs[], const idx_t &count, vector<unique_ptr<data_t[]>> &columns) {
	const uint32_t *row_ids = (uint32_t *)idxs;
	vector<T *> cols;
	for (const auto &col : columns) {
		cols.push_back((T *)col.get());
	}
	for (idx_t i = 0; i < count - 1; i++) {
		const auto &l_id = row_ids[i];
		const auto &r_id = row_ids[i + 1];
		for (const auto &col : cols) {
			if (col[l_id] < col[r_id]) {
				break;
			} else if (col[l_id] == col[r_id]) {
				continue;
			} else {
				assert(false);
			}
		}
	}
}

template <class T>
string SimulateColumnComparator(const idx_t &count, const idx_t &columns, string category) {
	// Initialize source data
	auto row_ids = InitRowIDs(count, false);
	auto source = AllocateColumns(count, columns, sizeof(T));
	FillColumns<T>(source, count, "skewed");

	auto before_timestamp = CurrentTime();
#ifdef TRACE
	this_thread::sleep_for(seconds(2));
#endif
	if (category == "col_ss") {
		SortColumnSubsort<T>(row_ids, source, count);
	} else {
		SortColumn<T>(row_ids, source, count, category);
	}
#ifdef TRACE
	this_thread::sleep_for(seconds(2));
#endif
	auto after_timestamp = CurrentTime();
	AssertSortedColumns<T>(row_ids.get(), count, source);

	vector<unique_ptr<data_t[]>> dummy_cols;
	dummy_cols.push_back(move(row_ids));
	// PrintColumns<T>(dummy_cols, count, 0);

	// Compute duration of phases
	auto total_duration = after_timestamp - before_timestamp;
	return CreateOutput(category, {count, columns, sizeof(T), total_duration, total_duration}, 8);
}

template <class T>
string SimulateComparator(idx_t count, idx_t columns) {
	ostringstream result;
	result << SimulateColumnComparator<T>(count, columns, "col") << endl;
	result << SimulateColumnComparator<T>(count, columns, "col_ss") << endl;
	result << SimulateColumnComparator<T>(count, columns, "col_branchless") << endl;
	result << SimulateRowComparator<T>(count, columns, "row_all") << endl;
	result << SimulateRowComparator<T>(count, columns, "row_iter") << endl;
	result << SimulateRowComparator<T>(count, columns, "row_norm") << endl;
	result << SimulateRowComparator<T>(count, columns, "row_all_branchless") << endl;
	return result.str();
}

template <class T>
void SimulateComparator(idx_t row_max, idx_t col_max, idx_t iterations) {
	cout << "SimulateComparator" << endl;
	ofstream results_file("results/comparator.csv", ios::trunc);
	results_file << CreateComparatorCSVHeader() << endl;
	for (idx_t r = 10; r < row_max; r += 2) {
		for (idx_t c = 1; c < col_max + 1; c++) {
			for (idx_t i = 0; i < iterations; i++) {
				results_file << SimulateComparator<T>(1 << r, c);
				results_file.flush();
				cout << "." << flush;
			}
		}
		cout << endl;
	}
}

//===--------------------------------------------------------------------===//
// Sorting Simulation
//===--------------------------------------------------------------------===//
string CreateSortCSVHeader() {
	return "category,count,columns,col_width,total,sort,scatter,gather";
}

void RadixSortLSD(data_ptr_t orig_ptr, const idx_t &count, const idx_t &row_width, const idx_t &comp_width) {
	auto temp_block = unique_ptr<data_t[]>(new data_t[count * row_width]);
	bool swap = false;

	idx_t counts[256];
	for (idx_t r = 1; r <= comp_width; r++) {
		// Init counts to 0
		memset(counts, 0, sizeof(counts));
		// Const some values for convenience
		const data_ptr_t source_ptr = swap ? temp_block.get() : orig_ptr;
		const data_ptr_t target_ptr = swap ? orig_ptr : temp_block.get();
		const idx_t offset = comp_width - r;
		// Collect counts
		data_ptr_t offset_ptr = source_ptr + offset;
		for (idx_t i = 0; i < count; i++) {
			counts[*offset_ptr]++;
			offset_ptr += row_width;
		}
		// Compute offsets from counts
		idx_t max_count = counts[0];
		for (idx_t val = 1; val < 256; val++) {
			max_count = max<idx_t>(max_count, counts[val]);
			counts[val] = counts[val] + counts[val - 1];
		}
		if (max_count == count) {
			continue;
		}
		// Re-order the data in temporary array
		data_ptr_t row_ptr = source_ptr + (count - 1) * row_width;
		for (idx_t i = 1; i <= count; i++) {
			idx_t &radix_offset = --counts[*(row_ptr + offset)];
			duckdb::fast_memcpy(target_ptr + radix_offset * row_width, row_ptr,
			                    row_width); // assignment is faster than memcpy
			row_ptr -= row_width;
		}
		swap = !swap;
	}
	// Move data back to original buffer (if it was swapped)
	if (swap) {
		duckdb::fast_memcpy(orig_ptr, temp_block.get(), count * row_width);
	}
}

inline void InsertionSort(const data_ptr_t orig_ptr, const data_ptr_t temp_ptr, const idx_t &count,
                          const idx_t &row_width, const idx_t &total_comp_width, const idx_t &offset, bool swap) {
	const data_ptr_t source_ptr = swap ? temp_ptr : orig_ptr;
	const data_ptr_t target_ptr = swap ? orig_ptr : temp_ptr;
	if (count > 1) {
		auto temp_val = unique_ptr<data_t[]>(new data_t[row_width]);
		const data_ptr_t val = temp_val.get();
		const auto comp_width = total_comp_width - offset;
		for (idx_t i = 1; i < count; i++) {
			duckdb::fast_memcpy(val, source_ptr + i * row_width, row_width);
			idx_t j = i;
			while (j > 0 &&
			       duckdb::fast_memcmp(source_ptr + (j - 1) * row_width + offset, val + offset, comp_width) > 0) {
				duckdb::fast_memcpy(source_ptr + j * row_width, source_ptr + (j - 1) * row_width, row_width);
				j--;
			}
			duckdb::fast_memcpy(source_ptr + j * row_width, val, row_width);
		}
	}
	if (swap) {
		memcpy(target_ptr, source_ptr, count * row_width);
	}
}

void RadixSortMSD(const data_ptr_t orig_ptr, const data_ptr_t temp_ptr, const idx_t &count, const idx_t &row_width,
                  const idx_t &comp_width, const idx_t offset, idx_t locations[], bool swap) {
	const data_ptr_t source_ptr = swap ? temp_ptr : orig_ptr;
	const data_ptr_t target_ptr = swap ? orig_ptr : temp_ptr;
	// Init counts to 0
	memset(locations, 0, 257 * sizeof(idx_t));
	idx_t *counts = locations + 1;
	// Collect counts
	data_ptr_t offset_ptr = source_ptr + offset;
	for (idx_t i = 0; i < count; i++) {
		counts[*offset_ptr]++;
		offset_ptr += row_width;
	}
	// Compute locations from counts
	idx_t max_count = 0;
	for (idx_t radix = 0; radix < 256; radix++) {
		max_count = max<idx_t>(max_count, counts[radix]);
		counts[radix] += locations[radix];
	}
	if (max_count != count) {
		// Re-order the data in temporary array
		data_ptr_t row_ptr = source_ptr;
		for (idx_t i = 0; i < count; i++) {
			const idx_t &radix_offset = locations[*(row_ptr + offset)]++;
			duckdb::fast_memcpy(target_ptr + radix_offset * row_width, row_ptr, row_width);
			row_ptr += row_width;
		}
		swap = !swap;
	}
	// Check if done
	if (offset == comp_width - 1) {
		if (swap) {
			duckdb::fast_memcpy(orig_ptr, temp_ptr, count * row_width);
		}
		return;
	}
	if (max_count == count) {
		RadixSortMSD(orig_ptr, temp_ptr, count, row_width, comp_width, offset + 1, locations + 257, swap);
		return;
	}
	// Recurse
	idx_t radix_count = locations[0];
	for (idx_t radix = 0; radix < 256; radix++) {
		const idx_t loc = (locations[radix] - radix_count) * row_width;
		if (radix_count > 24) {
			RadixSortMSD(orig_ptr + loc, temp_ptr + loc, radix_count, row_width, comp_width, offset + 1,
			             locations + 257, swap);
		} else if (radix_count != 0) {
			InsertionSort(orig_ptr + loc, temp_ptr + loc, radix_count, row_width, comp_width, offset + 1, swap);
		}
		radix_count = locations[radix + 1] - locations[radix];
	}
}

void RadixSort(const data_ptr_t source_ptr, const idx_t &count, const idx_t &row_width, const idx_t &comp_width) {
	if (count <= 24) {
		InsertionSort(source_ptr, nullptr, count, row_width, comp_width, 0, false);
	} else if (comp_width <= 4) {
		RadixSortLSD(source_ptr, count, row_width, comp_width);
	} else {
		auto target_block = unique_ptr<data_t[]>(new data_t[count * row_width]);
		auto preallocated_array = unique_ptr<idx_t[]>(new idx_t[comp_width * 257]);
		RadixSortMSD(source_ptr, target_block.get(), count, row_width, comp_width, 0, preallocated_array.get(), false);
	}
}

void AssertSorted(data_ptr_t ptr, const idx_t &count, const idx_t &row_width, const idx_t &comp_width) {
	for (idx_t i = 0; i < count - 1; i++) {
		assert(memcmp(ptr, ptr + row_width, comp_width) <= 0);
		ptr += row_width;
	}
}

void VerifySort() {
	idx_t count = 30;
	idx_t columns = 5;

	// Initialize source data
	auto row_ids = InitRowIDs(count, false);
	auto source = AllocateColumns(count, columns, sizeof(uint32_t));
	FillColumns<uint32_t>(source, count, "skewed");
	source.push_back(move(row_ids));
	cout << "--- BEFORE --- " << endl;
	PrintColumns<uint32_t>(source, count, columns);

	idx_t row_width = 0;
	vector<idx_t> col_widths;
	vector<bool> radix;
	for (idx_t i = 0; i < columns; i++) {
		row_width += sizeof(uint32_t);
		col_widths.push_back(sizeof(uint32_t));
		radix.push_back(true);
	}
	row_width += sizeof(uint32_t);
	col_widths.push_back(sizeof(uint32_t));
	radix.push_back(false);

	cout << "--- SCATTERED --- " << endl;
	auto row_data = Scatter<uint32_t>(move(source), count, row_width, col_widths, radix);
	auto scatter_timestamp = CurrentTime();
	PrintRows<uint32_t>(row_data.get(), count, col_widths, true);

	RadixSort(row_data.get(), count, row_width, row_width - sizeof(uint32_t));
	cout << "--- SORTED --- " << endl;
	PrintRows<uint32_t>(row_data.get(), count, col_widths, true);

	AssertSorted(row_data.get(), count, row_width, row_width - sizeof(uint32_t));
}

template <class T>
string SimulateSortInternal(const idx_t &count, const idx_t &columns, string method) {
	// This must hold for for alignment
	assert((columns * sizeof(T)) % sizeof(uint32_t) == 0);
	assert(method == "radix" || method == "pdq_static" || method == "pdq_dynamic");

	// Initialize source data
	auto row_ids = InitRowIDs(count, false);
	auto source = AllocateColumns(count, columns, sizeof(T));
	FillColumns<T>(source, count, "skewed");
	source.push_back(move(row_ids));

	auto before_timestamp = CurrentTime();
	idx_t row_width = 0;
	vector<idx_t> col_widths;
	vector<bool> radix;
	for (idx_t i = 0; i < columns; i++) {
		row_width += sizeof(T);
		col_widths.push_back(sizeof(T));
		radix.push_back(true);
	}
	row_width += sizeof(uint32_t);
	col_widths.push_back(sizeof(uint32_t));
	radix.push_back(false);

	auto row_data = Scatter<T>(move(source), count, row_width, col_widths, radix);
	auto scatter_timestamp = CurrentTime();

#ifdef TRACE
	this_thread::sleep_for(seconds(2));
#endif
	if (method == "radix") {
		RadixSort(row_data.get(), count, row_width, row_width - sizeof(uint32_t));
	} else {
		SortRowNormalized<T>(row_data.get(), count, columns, method);
	}
#ifdef TRACE
	this_thread::sleep_for(seconds(2));
#endif
	auto sort_timestamp = CurrentTime();
	AssertSorted(row_data.get(), count, row_width, row_width - sizeof(uint32_t));

	auto target = GatherRowID(row_data, count, row_width, row_width - sizeof(uint32_t));
	auto after_timestamp = CurrentTime();

	// Compute duration of phases
	auto total_duration = after_timestamp - before_timestamp;
	auto scatter_duration = scatter_timestamp - before_timestamp;
	auto sort_duration = sort_timestamp - scatter_timestamp;
	auto gather_duration = after_timestamp - sort_timestamp;
	return CreateOutput(
	    method, {count, columns, sizeof(T), total_duration, sort_duration, scatter_duration, gather_duration}, 8);
}

template <class T>
string SimulateSort(idx_t count, idx_t columns) {
	ostringstream result;
	result << SimulateSortInternal<T>(count, columns, "radix") << endl;
	// result << SimulateSortInternal<T>(count, columns, "pdq_dynamic") << endl;
	result << SimulateSortInternal<T>(count, columns, "pdq_static") << endl;
	return result.str();
}

template <class T>
void SimulateSort(idx_t row_max, idx_t col_max, idx_t iterations) {
	cout << "SimulateSort" << endl;
	ofstream results_file("results/sort.csv", ios::trunc);
	results_file << CreateSortCSVHeader() << endl;
	for (idx_t r = 10; r < row_max; r += 2) {
		for (idx_t c = 1; c < col_max + 1; c++) {
			for (idx_t i = 0; i < iterations; i++) {
				results_file << SimulateSort<T>(1 << r, c);
				results_file.flush();
				cout << "." << flush;
			}
		}
		cout << endl;
	}
}

//===--------------------------------------------------------------------===//
// Key Merge Simulation
//===--------------------------------------------------------------------===//
string CreateMergeKeyCSVHeader() {
	return "category,count,columns,col_width,total";
}

template <class T>
unique_ptr<bool[]> MergeKeyColumns(const vector<unique_ptr<data_t[]>> &left, const vector<unique_ptr<data_t[]>> &right,
                                   const idx_t &count, bool branchless) {
	assert(left.size() == right.size());
	const idx_t columns = left.size();
	vector<const T *> l_cols;
	vector<const T *> r_cols;
	for (idx_t col_idx = 0; col_idx < columns; col_idx++) {
		l_cols.push_back((T *)left[col_idx].get());
		r_cols.push_back((T *)right[col_idx].get());
	}
	idx_t l_i = 0;
	idx_t r_i = 0;
	idx_t result_i = 0;
	auto left_smaller_ptr = unique_ptr<bool[]>(new bool[count * 2]);
	auto left_smaller = left_smaller_ptr.get();
	if (branchless) {
		while (l_i < count && r_i < count) {
			left_smaller[result_i] = CompareColumnarBranchless<T>(l_cols, r_cols, l_i, r_i, columns);
			l_i += left_smaller[result_i];
			r_i += !left_smaller[result_i];
			result_i++;
		}
	} else {
		while (l_i < count && r_i < count) {
			left_smaller[result_i] = CompareColumnarBranch<T>(l_cols, r_cols, l_i, r_i, columns);
			l_i += left_smaller[result_i];
			r_i += !left_smaller[result_i];
			result_i++;
		}
	}
	const bool left_rest_smaller = l_i != count;
	for (; result_i < 2 * count; result_i++) {
		left_smaller[result_i] = left_rest_smaller;
	}
	return left_smaller_ptr;
}

template <class T>
unique_ptr<bool[]> MergeNormalizedKeyRows(data_ptr_t left, data_ptr_t right, const idx_t &count, const idx_t &columns) {
	const idx_t row_width = columns * sizeof(T) + sizeof(uint32_t);
	const idx_t comp_width = row_width - sizeof(uint32_t);
	const data_ptr_t l_end = left + count * row_width;
	const data_ptr_t r_end = right + count * row_width;
	idx_t result_i = 0;
	auto left_smaller_ptr = unique_ptr<bool[]>(new bool[count * 2]);
	auto left_smaller = left_smaller_ptr.get();
	while (left != l_end && right != r_end) {
		left_smaller[result_i] = duckdb::fast_memcmp(left, right, comp_width) < 0;
		left += left_smaller[result_i] * row_width;
		right += !left_smaller[result_i] * row_width;
		result_i++;
	}
	const bool left_rest_smaller = left != l_end;
	for (; result_i < 2 * count; result_i++) {
		left_smaller[result_i] = left_rest_smaller;
	}
	return left_smaller_ptr;
}

template <class ROW>
unique_ptr<bool[]> MergeKeyRows(data_ptr_t l_ptr, data_ptr_t r_ptr, const idx_t &count) {
	idx_t l_i = 0;
	idx_t r_i = 0;
	idx_t result_i = 0;
	auto left = (ROW *)l_ptr;
	auto right = (ROW *)r_ptr;
	auto left_smaller_ptr = unique_ptr<bool[]>(new bool[count * 2]);
	auto left_smaller = left_smaller_ptr.get();
	for (; l_i < count && r_i < count; result_i++) {
		left_smaller[result_i] = left[l_i] < right[r_i];
		l_i += left_smaller[result_i];
		r_i += !left_smaller[result_i];
	}
	const bool left_rest_smaller = l_i < count;
	for (; result_i < 2 * count; result_i++) {
		left_smaller[result_i] = left_rest_smaller;
	}
	return left_smaller_ptr;
}

template <class T>
unique_ptr<bool[]> MergeKeyRows(data_ptr_t left, data_ptr_t right, const idx_t &count, const idx_t columns,
                                string method) {
	if (method == "row_all" || method == "row") {
		switch (columns) {
		case 1:
			return MergeKeyRows<BranchedRowOrderEntry1<T>>(left, right, count);
		case 2:
			return MergeKeyRows<BranchedRowOrderEntry2<T>>(left, right, count);
		case 3:
			return MergeKeyRows<BranchedRowOrderEntry3<T>>(left, right, count);
		case 4:
			return MergeKeyRows<BranchedRowOrderEntry4<T>>(left, right, count);
		case 5:
			return MergeKeyRows<BranchedRowOrderEntry5<T>>(left, right, count);
		case 6:
			return MergeKeyRows<BranchedRowOrderEntry6<T>>(left, right, count);
		case 7:
			return MergeKeyRows<BranchedRowOrderEntry7<T>>(left, right, count);
		default:
			assert(false);
		}
	} else if (method == "row_all_branchless") {
		switch (columns) {
		case 1:
			return MergeKeyRows<BranchlessRowOrderEntry1<T>>(left, right, count);
		case 2:
			return MergeKeyRows<BranchlessRowOrderEntry2<T>>(left, right, count);
		case 3:
			return MergeKeyRows<BranchlessRowOrderEntry3<T>>(left, right, count);
		case 4:
			return MergeKeyRows<BranchlessRowOrderEntry4<T>>(left, right, count);
		case 5:
			return MergeKeyRows<BranchlessRowOrderEntry5<T>>(left, right, count);
		case 6:
			return MergeKeyRows<BranchlessRowOrderEntry6<T>>(left, right, count);
		case 7:
			return MergeKeyRows<BranchlessRowOrderEntry7<T>>(left, right, count);
		default:
			assert(false);
		}
	} else if (method == "row_norm") {
		// Normalized key has to have specialized code so memcmp can be inlined
		return MergeNormalizedKeyRows<T>(left, right, count, columns);
	} else {
		assert(false);
	}
}

template <class T>
string SimulateColumnKeyMerge(const idx_t &count, const idx_t &columns, bool branchless) {
	// Initialize source data
	auto left = AllocateColumns(count, columns, sizeof(T));
	auto right = AllocateColumns(count, columns, sizeof(T));
	FillColumns<T>(left, count, "skewed");
	FillColumns<T>(right, count, "skewed");

	auto row_ids = InitRowIDs(count, false);
	SortColumnSubsort<T>(row_ids, left, count);
	left = ReOrderColumns<T>((uint32_t *)row_ids.get(), left, count);

	row_ids = InitRowIDs(count, false);
	SortColumnSubsort<T>(row_ids, right, count);
	right = ReOrderColumns<T>((uint32_t *)row_ids.get(), right, count);

	// Merge
	auto before_timestamp = CurrentTime();
#ifdef TRACE
	this_thread::sleep_for(seconds(2));
#endif
	MergeKeyColumns<T>(left, right, count, branchless);
#ifdef TRACE
	this_thread::sleep_for(seconds(2));
#endif
	auto after_timestamp = CurrentTime();

	// Compute duration of phases
	auto total_duration = after_timestamp - before_timestamp;
	string category = branchless ? "col_branchless" : "col_branch";
	return CreateOutput(category, {count, columns, sizeof(T), total_duration}, 5);
}

template <class T>
string SimulateRowKeyMerge(const idx_t &count, const idx_t &columns, string category) {
	auto row_ids = InitRowIDs(count, false);
	// Initialize source data
	auto l_row_ids = InitRowIDs(count, false);
	auto r_row_ids = InitRowIDs(count, false);
	auto left = AllocateColumns(count, columns, sizeof(T));
	auto right = AllocateColumns(count, columns, sizeof(T));
	left.push_back(move(l_row_ids));
	right.push_back(move(r_row_ids));
	FillColumns<T>(left, count, "skewed");
	FillColumns<T>(right, count, "skewed");

	idx_t row_width = 0;
	vector<idx_t> col_widths;
	vector<bool> radix;
	for (idx_t i = 0; i < columns; i++) {
		row_width += sizeof(T);
		col_widths.push_back(sizeof(T));
		radix.push_back(category == "row_norm");
	}
	row_width += sizeof(uint32_t);
	col_widths.push_back(sizeof(uint32_t));
	radix.push_back(false);

	auto left_rows = Scatter<uint32_t>(move(left), count, row_width, col_widths, radix);
	auto right_rows = Scatter<uint32_t>(move(right), count, row_width, col_widths, radix);
	SortRowNormalized<T>(left_rows.get(), count, columns, "pdq_static");
	SortRowNormalized<T>(right_rows.get(), count, columns, "pdq_static");

	// Merge
	auto before_timestamp = CurrentTime();
#ifdef TRACE
	this_thread::sleep_for(seconds(2));
#endif
	MergeKeyRows<T>(left_rows.get(), right_rows.get(), count, columns, category);
#ifdef TRACE
	this_thread::sleep_for(seconds(2));
#endif
	auto after_timestamp = CurrentTime();

	// Compute duration of phases
	auto total_duration = after_timestamp - before_timestamp;
	return CreateOutput(category, {count, columns, sizeof(T), total_duration}, 5);
}

template <class T>
string SimulateKeyMerge(idx_t count, idx_t columns) {
	ostringstream result;
	result << SimulateColumnKeyMerge<T>(count, columns, true) << endl;
	result << SimulateColumnKeyMerge<T>(count, columns, false) << endl;
	result << SimulateRowKeyMerge<T>(count, columns, "row_all") << endl;
	result << SimulateRowKeyMerge<T>(count, columns, "row_all_branchless") << endl;
	result << SimulateRowKeyMerge<T>(count, columns, "row_norm") << endl;
	return result.str();
}

template <class T>
void SimulateKeyMerge(idx_t row_max, idx_t col_max, idx_t iterations) {
	cout << "SimulateKeyMerge" << endl;
	ofstream results_file("results/merge_key.csv", ios::trunc);
	results_file << CreateMergeKeyCSVHeader() << endl;
	for (idx_t r = 10; r < row_max; r += 2) {
		for (idx_t c = 1; c < col_max + 1; c++) {
			for (idx_t i = 0; i < iterations; i++) {
				results_file << SimulateKeyMerge<T>(1 << r, c);
				results_file.flush();
				cout << "." << flush;
			}
		}
		cout << endl;
	}
}

//===--------------------------------------------------------------------===//
// Payload Merge Simulation
//===--------------------------------------------------------------------===//
string CreateMergePayloadCSVHeader() {
	return "category,count,columns,col_width,total";
}

template <class T>
vector<unique_ptr<data_t[]>> MergePayloadColumns(const bool left_smaller[], vector<unique_ptr<data_t[]>> left,
                                                 vector<unique_ptr<data_t[]>> right, const idx_t &count) {
	auto result = AllocateColumns(count * 2, left.size(), sizeof(T));
	idx_t l_i = 0;
	idx_t r_i = 0;
	idx_t result_i = 0;
	while (l_i != count || r_i != count) {
		if (l_i == count) {
			idx_t entries = count - r_i;
			for (idx_t column = 0; column < right.size(); column++) {
				data_ptr_t r_ptr = right[column].get() + r_i * sizeof(T);
				data_ptr_t target_ptr = result[column].get() + result_i * sizeof(T);
				memcpy(target_ptr, r_ptr, entries * sizeof(T));
			}
			result_i += entries;
			r_i += entries;
		} else if (r_i == count) {
			idx_t entries = count - l_i;
			for (idx_t column = 0; column < left.size(); column++) {
				data_ptr_t l_ptr = left[column].get() + l_i * sizeof(T);
				data_ptr_t target_ptr = result[column].get() + result_i * sizeof(T);
				memcpy(target_ptr, l_ptr, entries * sizeof(T));
			}
			result_i += entries;
			l_i += entries;
		} else {
			idx_t l_i_temp = l_i;
			idx_t r_i_temp = r_i;
			idx_t j;
			for (j = 0; j < STANDARD_VECTOR_SIZE && l_i_temp < count && r_i_temp < count; j++) {
				l_i_temp += left_smaller[j];
				r_i_temp += !left_smaller[j];
			}
			const idx_t next = j;
			for (idx_t column = 0; column < left.size(); column++) {
				T *l = (T *)left[column].get() + l_i;
				T *r = (T *)right[column].get() + r_i;
				T *target = (T *)result[column].get() + result_i;
				for (j = 0; j < next; j++) {
					const bool &copy_left = left_smaller[j];
					const bool copy_right = !copy_left;
					target[j] = *(T *)(copy_left * (idx_t)l + copy_right * (idx_t)r);
					l += copy_left;
					r += copy_right;
				}
			}
			result_i += next;
			l_i = l_i_temp;
			r_i = r_i_temp;
		}
	}
	return result;
}

unique_ptr<data_t[]> MergePayloadRows(const bool left_smaller[], unique_ptr<data_t[]> left, unique_ptr<data_t[]> right,
                                      const idx_t &count, const idx_t &row_width) {
	auto result = AllocateRows(count * 2, row_width);
	idx_t l_i = 0;
	idx_t r_i = 0;
	idx_t result_i = 0;
	while (l_i != count || r_i != count) {
		if (l_i == count) {
			idx_t entries = count - r_i;
			data_ptr_t r_ptr = right.get() + r_i * row_width;
			data_ptr_t target_ptr = result.get() + result_i * row_width;
			memcpy(target_ptr, r_ptr, entries * row_width);
			result_i += entries;
			r_i += entries;
		} else if (r_i == count) {
			idx_t entries = count - l_i;
			data_ptr_t l_ptr = left.get() + l_i * row_width;
			data_ptr_t target_ptr = result.get() + result_i * row_width;
			memcpy(target_ptr, l_ptr, entries * row_width);
			result_i += entries;
			l_i += entries;
		} else {
			idx_t l_i_temp = l_i;
			idx_t r_i_temp = r_i;
			idx_t j;
			for (j = 0; j < STANDARD_VECTOR_SIZE && l_i_temp < count && r_i_temp < count; j++) {
				l_i_temp += left_smaller[j];
				r_i_temp += !left_smaller[j];
			}
			const idx_t next = j;
			data_ptr_t l_ptr = left.get() + l_i * row_width;
			data_ptr_t r_ptr = right.get() + r_i * row_width;
			data_ptr_t target_ptr = result.get() + result_i * row_width;
			for (j = 0; j < next; j++) {
				const bool &copy_left = left_smaller[j];
				const bool copy_right = !copy_left;
				duckdb::fast_memcpy(target_ptr, (data_ptr_t)(copy_left * (idx_t)l_ptr + copy_right * (idx_t)r_ptr),
				                    row_width);
				target_ptr += row_width;
				l_ptr += copy_left * row_width;
				r_ptr += copy_right * row_width;
			}
			result_i += next;
			l_i = l_i_temp;
			r_i = r_i_temp;
		}
	}
	return result;
}

unique_ptr<bool[]> GenerateLeftSmaller(const idx_t &count) {
	auto result = unique_ptr<bool[]>(new bool[count * 2]);
	auto left_smaller = result.get();
	idx_t i;
	idx_t l_count = 0;
	idx_t r_count = 0;
	for (i = 0; l_count < count && r_count < count; i++) {
		auto &l_smaller_i = left_smaller[i];
		l_smaller_i = ((double)rand() / (RAND_MAX)) < 0.5;
		l_count += l_smaller_i;
		r_count += !l_smaller_i;
	}
	if (l_count < count) {
		for (; l_count < count; i++) {
			left_smaller[i] = true;
			l_count++;
		}
	} else {
		for (; r_count < count; i++) {
			left_smaller[i] = false;
			r_count++;
		}
	}
	return result;
}

template <class T>
string SimulateColumnPayloadMerge(const idx_t &count, const idx_t &columns) {
	// Initialize source data
	auto left = AllocateColumns(count / 2, columns, sizeof(T));
	auto right = AllocateColumns(count / 2, columns, sizeof(T));

	// Generate bool array
	auto left_smaller = GenerateLeftSmaller(count);

	// Merge
	auto before_timestamp = CurrentTime();
#ifdef TRACE
	this_thread::sleep_for(seconds(2));
#endif
	auto target = MergePayloadColumns<T>(left_smaller.get(), move(left), move(right), count / 2);
#ifdef TRACE
	this_thread::sleep_for(seconds(2));
#endif
	auto after_timestamp = CurrentTime();

	// Compute duration of phases
	auto total_duration = after_timestamp - before_timestamp;
	return CreateOutput("col", {count, columns, sizeof(T), total_duration}, 5);
}

template <class T>
string SimulateRowPayloadMerge(const idx_t &count, const idx_t &columns) {
	// Initialize source data
	const idx_t row_width = columns * sizeof(T);
	auto left = AllocateRows(count / 2, row_width);
	auto right = AllocateRows(count / 2, row_width);

	// Generate bool array
	auto left_smaller = GenerateLeftSmaller(count);

	// Merge
	auto before_timestamp = CurrentTime();
#ifdef TRACE
	this_thread::sleep_for(seconds(2));
#endif
	auto target = MergePayloadRows(left_smaller.get(), move(left), move(right), count / 2, row_width);
#ifdef TRACE
	this_thread::sleep_for(seconds(2));
#endif
	auto after_timestamp = CurrentTime();

	// Compute duration of phases
	auto total_duration = after_timestamp - before_timestamp;
	return CreateOutput("row", {count, columns, sizeof(T), total_duration}, 5);
}

template <class T>
string SimulatePayloadMerge(idx_t count, idx_t columns) {
	ostringstream result;
	result << SimulateColumnPayloadMerge<T>(count, columns) << endl;
	result << SimulateRowPayloadMerge<T>(count, columns) << endl;
	return result.str();
}

template <class T>
void SimulatePayloadMerge(idx_t row_max, idx_t col_max, idx_t iterations) {
	cout << "SimulatePayloadMerge" << endl;
	ofstream results_file("results/merge_payload.csv", ios::trunc);
	results_file << CreateMergePayloadCSVHeader() << endl;
	for (idx_t r = 10; r < row_max; r += 2) {
		for (idx_t c = 0; c < col_max; c++) {
			for (idx_t i = 0; i < iterations; i++) {
				idx_t num_cols = min<idx_t>(1 << c, 96);
				results_file << SimulatePayloadMerge<T>(1 << r, num_cols);
				results_file.flush();
				cout << "." << flush;
			}
		}
		cout << endl;
	}
}

//===--------------------------------------------------------------------===//
// Fast Memcpy Simulation
//===--------------------------------------------------------------------===//
void SimulateFastMemcpy() {
	ofstream output("results/memcpy.csv", ios::trunc);
	output << "num_bytes,type,time" << endl;

	const idx_t max_bytes = 64;
	const idx_t rep = 1000000;
	auto ptr = unique_ptr<data_t[]>(new data_t[max_bytes * 2]);
	const data_ptr_t l_ptr = ptr.get();
	const data_ptr_t r_ptr = ptr.get() + max_bytes;
	for (idx_t size = 0; size <= max_bytes; size++) {
		auto before_timestamp = CurrentTime();
		for (idx_t r = 0; r < rep; r++) {
			memcpy(l_ptr, r_ptr, size);
		}
		auto after_timestamp = CurrentTime();
		double memcpy_time = (double)(after_timestamp - before_timestamp) / rep;

		before_timestamp = CurrentTime();
		for (idx_t r = 0; r < rep; r++) {
			duckdb::fast_memcpy(l_ptr, r_ptr, size);
		}
		after_timestamp = CurrentTime();
		double fast_memcpy_time = (double)(after_timestamp - before_timestamp) / rep;

		output << size << ",dynamic," << memcpy_time << endl;
		output << size << ",static," << fast_memcpy_time << endl;
	}
}

//===--------------------------------------------------------------------===//
// Fast Memcmp Simulation
//===--------------------------------------------------------------------===//
void SimulateFastMemcmp() {
	ofstream output("results/memcmp.csv", ios::trunc);
	output << "num_bytes,type,time" << endl;

	const idx_t max_bytes = 64;
	const idx_t rep = 1000000;

	auto ptr = unique_ptr<data_t[]>(new data_t[max_bytes * rep * 2]);
	const data_ptr_t l_ptr = ptr.get();
	const data_ptr_t r_ptr = ptr.get() + max_bytes * rep;

	for (idx_t size = 0; size <= max_bytes; size++) {
		data_ptr_t l_ptr_temp = l_ptr;
		data_ptr_t r_ptr_temp = r_ptr;
		for (idx_t i = 0; i < rep; i++) {
			// Fill with random data
			const idx_t same_until = ((idx_t)rand() % (size + 1));
			for (idx_t c = 0; c < same_until; c++) {
				l_ptr_temp[c] = 42;
				r_ptr_temp[c] = 42;
			}
			l_ptr_temp[same_until] = 13;
			r_ptr_temp[same_until] = 37;
			for (idx_t c = same_until + 1; c < max_bytes; c++) {
				l_ptr_temp[c] = 42;
				r_ptr_temp[c] = 42;
			}
			l_ptr_temp += max_bytes;
			r_ptr_temp += max_bytes;
		}

		l_ptr_temp = l_ptr;
		r_ptr_temp = r_ptr;
		int memcmp_checksum = 0;
		auto before_timestamp = CurrentTime();
		for (idx_t r = 0; r < rep; r++) {
			memcmp_checksum += memcmp(l_ptr_temp, r_ptr_temp, size);
			memcmp_checksum += memcmp(r_ptr_temp, l_ptr_temp, size);
			l_ptr_temp += max_bytes;
			r_ptr_temp += max_bytes;
		}
		auto after_timestamp = CurrentTime();
		double memcmp_time = (double)(after_timestamp - before_timestamp) / rep;

		l_ptr_temp = l_ptr;
		r_ptr_temp = r_ptr;
		int fast_memcmp_checksum = 0;
		before_timestamp = CurrentTime();
		for (idx_t r = 0; r < rep; r++) {
			fast_memcmp_checksum += duckdb::fast_memcmp(l_ptr_temp, r_ptr_temp, size);
			fast_memcmp_checksum += duckdb::fast_memcmp(r_ptr_temp, l_ptr_temp, size);
			l_ptr_temp += max_bytes;
			r_ptr_temp += max_bytes;
		}
		after_timestamp = CurrentTime();
		double fast_memcmp_time = (double)(after_timestamp - before_timestamp) / rep;

		assert(memcmp_checksum == fast_memcmp_checksum);

		output << size << ",dynamic," << memcmp_time << endl;
		output << size << ",static," << fast_memcmp_time << endl;
	}
}

//===--------------------------------------------------------------------===//
// End-to-End
//===--------------------------------------------------------------------===//
string CreateEndToEndCSVHeader() {
	return "category,count,key_columns,payload_columns,col_width,init,scatter,sort,reorder,merge_key,merge_payload,"
	       "gather,total";
}

template <class T>
string EndToEndColumnar(idx_t count, idx_t key_columns, idx_t payload_columns) {
	auto before_timestamp = CurrentTime();

	const idx_t num_runs = 4;
	vector<unique_ptr<data_t[]>> run_row_ids;
	vector<vector<unique_ptr<data_t[]>>> key_column_data;
	vector<vector<unique_ptr<data_t[]>>> payload_column_data;
	for (idx_t run = 0; run < num_runs; run++) {
		run_row_ids.push_back(InitRowIDs(count, false));
		key_column_data.push_back(AllocateColumns(count, key_columns, sizeof(T)));
		FillColumns<T>(key_column_data.back(), count, "skewed");
		payload_column_data.push_back(AllocateColumns(count, payload_columns, sizeof(T)));
	}

	auto init_timestamp = CurrentTime();

	for (idx_t run = 0; run < num_runs; run++) {
		SortColumnSubsort<T>(run_row_ids[run], key_column_data[run], count);
	}

	auto sort_timestamp = CurrentTime();

	for (idx_t run = 0; run < num_runs; run++) {
		key_column_data[run] = ReOrderColumns<T>((uint32_t *)run_row_ids[run].get(), key_column_data[run], count);
		payload_column_data[run] =
		    ReOrderColumns<T>((uint32_t *)run_row_ids[run].get(), payload_column_data[run], count);
	}

	auto reorder_timestamp = CurrentTime();

	idx_t merge_key_duration = 0;
	idx_t merge_payload_duration = 0;
	while (key_column_data.size() > 1) {
		idx_t actual_count = key_column_data.size() == 2 ? 2 * count : count;

		auto merge_before_timestamp = CurrentTime();

		auto left_key_cols = move(key_column_data.back());
		key_column_data.pop_back();
		auto right_key_cols = move(key_column_data.back());
		key_column_data.pop_back();
		auto left_smaller = MergeKeyColumns<T>(left_key_cols, right_key_cols, actual_count, false);

		auto merge_key_timestamp = CurrentTime();

		key_column_data.insert(key_column_data.begin(), MergePayloadColumns<T>(left_smaller.get(), move(left_key_cols),
		                                                                       move(right_key_cols), actual_count));

		auto left_payload_cols = move(payload_column_data.back());
		payload_column_data.pop_back();
		auto right_payload_cols = move(payload_column_data.back());
		payload_column_data.pop_back();
		payload_column_data.insert(payload_column_data.begin(),
		                           MergePayloadColumns<T>(left_smaller.get(), move(left_payload_cols),
		                                                  move(right_payload_cols), actual_count));

		auto merge_payload_timestamp = CurrentTime();

		merge_key_duration += merge_key_timestamp - merge_before_timestamp;
		merge_payload_duration += merge_payload_timestamp - merge_key_timestamp;
	}

	auto after_timestamp = CurrentTime();

	auto init_duration = init_timestamp - before_timestamp;
	idx_t scatter_duration = 0;
	auto sort_duration = sort_timestamp - init_timestamp;
	auto reorder_duration = reorder_timestamp - sort_timestamp;
	idx_t gather_duration = 0;
	auto total_duration = after_timestamp - before_timestamp;
	return CreateOutput("col",
	                    {count, key_columns, payload_columns, sizeof(T), init_duration, scatter_duration, sort_duration,
	                     reorder_duration, merge_key_duration, merge_payload_duration, gather_duration, total_duration},
	                    13);
}

template <class T>
string EndToEndRow(idx_t count, idx_t key_columns, idx_t payload_columns, string category) {
	auto before_timestamp = CurrentTime();

	const idx_t num_runs = 4;
	vector<unique_ptr<data_t[]>> run_row_ids;
	vector<vector<unique_ptr<data_t[]>>> key_column_data;
	vector<vector<unique_ptr<data_t[]>>> payload_column_data;
	for (idx_t run = 0; run < num_runs; run++) {
		run_row_ids.push_back(InitRowIDs(count, false));
		key_column_data.push_back(AllocateColumns(count, key_columns, sizeof(T)));
		FillColumns<T>(key_column_data.back(), count, "skewed");
		payload_column_data.push_back(AllocateColumns(count, payload_columns, sizeof(T)));
	}

	auto init_timestamp = CurrentTime();

	vector<unique_ptr<data_t[]>> key_row_data;
	idx_t key_row_width = 0;
	{
		vector<idx_t> col_widths;
		vector<bool> radix;
		for (idx_t i = 0; i < key_columns; i++) {
			key_row_width += sizeof(T);
			col_widths.push_back(sizeof(T));
			radix.push_back(category == "row_norm");
		}
		key_row_width += sizeof(uint32_t);
		col_widths.push_back(sizeof(uint32_t));
		radix.push_back(false);

		for (idx_t run = 0; run < num_runs; run++) {
			key_column_data[run].push_back(move(run_row_ids[run]));
			key_row_data.push_back(Scatter<T>(move(key_column_data[run]), count, key_row_width, col_widths, radix));
		}
	}

	vector<unique_ptr<data_t[]>> payload_row_data;
	idx_t payload_row_width = 0;
	vector<idx_t> payload_col_widths;
	{
		vector<bool> radix;
		for (idx_t i = 0; i < payload_columns; i++) {
			payload_row_width += sizeof(T);
			payload_col_widths.push_back(sizeof(T));
			radix.push_back(false);
		}

		for (idx_t run = 0; run < num_runs; run++) {
			payload_row_data.push_back(
			    Scatter<T>(move(payload_column_data[run]), count, payload_row_width, payload_col_widths, radix));
		}
	}

	auto scatter_timestamp = CurrentTime();

	for (idx_t run = 0; run < num_runs; run++) {
		if (category == "row_norm") {
			RadixSort(key_row_data[run].get(), count, key_row_width, key_row_width - sizeof(uint32_t));
		} else if (category == "row") {
			SortRowSubsort<T>(key_row_data[run].get(), count, key_columns);
		} else {
			assert(false);
		}
	}

	auto sort_timestamp = CurrentTime();

	for (idx_t run = 0; run < num_runs; run++) {
		auto row_ids = GatherRowID(key_row_data[run], count, key_row_width, key_row_width - sizeof(uint32_t));
		payload_row_data[run] =
		    ReOrderRows((uint32_t *)row_ids.get(), move(payload_row_data[run]), count, payload_row_width);
	}

	auto reorder_timestamp = CurrentTime();

	idx_t merge_key_duration = 0;
	idx_t merge_payload_duration = 0;
	while (key_row_data.size() > 1) {
		idx_t actual_count = key_row_data.size() == 2 ? 2 * count : count;

		auto merge_before_timestamp = CurrentTime();

		auto left_key_rows = move(key_row_data.back());
		key_row_data.pop_back();
		auto right_key_rows = move(key_row_data.back());
		key_row_data.pop_back();
		auto left_smaller =
		    MergeKeyRows<T>(left_key_rows.get(), right_key_rows.get(), actual_count, key_columns, category);

		auto merge_key_timestamp = CurrentTime();

		key_row_data.insert(key_row_data.begin(), MergePayloadRows(left_smaller.get(), move(left_key_rows),
		                                                           move(right_key_rows), actual_count, key_row_width));

		auto left_payload_rows = move(payload_row_data.back());
		payload_row_data.pop_back();
		auto right_payload_rows = move(payload_row_data.back());
		payload_row_data.pop_back();
		payload_row_data.insert(payload_row_data.begin(),
		                        MergePayloadRows(left_smaller.get(), move(left_payload_rows), move(right_payload_rows),
		                                         actual_count, payload_row_width));

		auto merge_payload_timestamp = CurrentTime();

		merge_key_duration += merge_key_timestamp - merge_before_timestamp;
		merge_payload_duration += merge_payload_timestamp - merge_key_timestamp;
	}

	auto merge_timestamp = CurrentTime();

	auto result = Gather<T>(move(payload_row_data[0]), 4 * count, payload_row_width, payload_col_widths);

	auto after_timestamp = CurrentTime();

	auto init_duration = init_timestamp - before_timestamp;
	auto scatter_duration = scatter_timestamp - init_timestamp;
	auto sort_duration = sort_timestamp - scatter_timestamp;
	auto reorder_duration = reorder_timestamp - sort_timestamp;
	auto gather_duration = after_timestamp - merge_timestamp;
	auto total_duration = after_timestamp - before_timestamp;
	return CreateOutput(category,
	                    {count, key_columns, payload_columns, sizeof(T), init_duration, scatter_duration, sort_duration,
	                     reorder_duration, merge_key_duration, merge_payload_duration, gather_duration, total_duration},
	                    13);
}

template <class T>
void SimulateEndToEnd(idx_t key_columns, idx_t payload_columns, idx_t count, idx_t rep) {
	cout << "SimulateEndToEnd" << endl;
	ofstream results_file("results/end_to_end.csv", ios::trunc);
	results_file << CreateEndToEndCSVHeader() << endl;
	for (idx_t i = 0; i < rep; i++) {
		results_file << EndToEndColumnar<T>(count, key_columns, payload_columns) << endl;
		cout << ".";
		results_file << EndToEndRow<T>(count, key_columns, payload_columns, "row") << endl;
		cout << ".";
		results_file << EndToEndRow<T>(count, key_columns, payload_columns, "row_norm") << endl;
		cout << "." << endl;
	}
}

//===--------------------------------------------------------------------===//
// Main
//===--------------------------------------------------------------------===//
template <class T>
void ParseArgs(int argc, char *argv[]) {
	auto sim = string(argv[1]);
	auto category = string(argv[2]);
	int count = stoi(argv[3]);
	int columns = stoi(argv[4]);
	cout << sim << " " << category << " " << count << " " << columns << endl;
	if (sim == "reorder") {
		auto row_ids = InitRowIDs(count, true);
		if (category == "row") {
			SimulateRowReOrder<T>((uint32_t *)row_ids.get(), count, columns);
			return;
		} else if (category == "col") {
			SimulateColumnReOrder<T>((uint32_t *)row_ids.get(), count, columns);
			return;
		}
	} else if (sim == "comparator") {
		if (category == "col_all" || category == "col_ss" || category == "col_branchless") {
			SimulateColumnComparator<T>(count, columns, category);
			return;
		} else if (category == "row_all" || category == "row_iter" || category == "row_norm" ||
		           category == "row_all_branchless") {
			SimulateRowComparator<T>(count, columns, category);
			return;
		}
	} else if (sim == "sort") {
		SimulateSortInternal<T>(count, columns, category);
		return;
	} else if (sim == "merge_key") {
		if (category == "row_all" || category == "row_all_branchless" || category == "row_norm") {
			SimulateRowKeyMerge<T>(count, columns, category);
			return;
		} else if (category == "col_branch") {
			SimulateColumnKeyMerge<T>(count, columns, false);
			return;
		} else if (category == "col_branchless") {
			SimulateColumnKeyMerge<T>(count, columns, true);
			return;
		}
	} else if (sim == "merge_payload") {
		if (category == "row") {
			SimulateRowPayloadMerge<T>(count, columns);
			return;
		} else if (category == "col") {
			SimulateColumnPayloadMerge<T>(count, columns);
			return;
		}
	}
	assert(false);
}

template <class T>
void Main(int argc, char *argv[]) {
	if (argc == 1) {
		// VerifyReOrder();
		// VerifySort();
		const idx_t row = 25;
		const idx_t col = 7;
		const idx_t rep = 5;
		SimulateReOrder<T>(row, col, rep);
		SimulateComparator<T>(row, col, rep);
		SimulateSort<T>(row, col, rep);
		SimulateKeyMerge<T>(row, col, rep);
		SimulatePayloadMerge<T>(row, col, rep);
		SimulateFastMemcpy();
		SimulateFastMemcmp();
		SimulateEndToEnd<T>(3, 32, (1 << 10), rep);
	} else {
		ParseArgs<T>(argc, argv);
	}
}

int main(int argc, char *argv[]) {
	Main<uint32_t>(argc, argv);
}
