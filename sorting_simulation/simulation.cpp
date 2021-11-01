#include <algorithm>
#include <assert.h>
#include <chrono>
#include <cstring>
#include <fstream>
#include <iostream>
#include <limits>
#include <memory>
#include <random>
#include <sstream>
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

template <class T>
unique_ptr<data_t[]> InitRowIDs(const idx_t &count, bool shuf) {
	auto result = unique_ptr<data_t[]>(new data_t[count * sizeof(T)]);
	auto order = (T *)result.get();
	for (T i = 0; i < count; i++) {
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
void FillColumns(vector<unique_ptr<data_t[]>> &columns, const idx_t &count, bool deterministic) {
	const idx_t max_val = min<idx_t>(numeric_limits<T>::max(), count);
	for (idx_t column = 0; column < columns.size(); column++) {
		T *col_ptr = (T *)columns[column].get();
		if (deterministic) {
			for (idx_t i = 0; i < count; i++) {
				col_ptr[i] = max_val - (((column + 1) * i) % max_val);
			}
		} else {
			const idx_t unique_vals = 1 << (3 + column);
			const idx_t gap = max_val / unique_vals;
			for (idx_t i = 0; i < count; i++) {
				col_ptr[i] = ((double)rand() / (RAND_MAX)) * unique_vals * gap;
			}
		}
	}
}

template <class ROWID, class T>
void PrintColumns(vector<unique_ptr<data_t[]>> &columns, const idx_t &count, const idx_t &num_cols) {
	ostringstream output;
	for (idx_t i = 0; i < count; i++) {
		for (idx_t column = 0; column < columns.size(); column++) {
			auto &col = columns[column];
			if (columns.size() > num_cols && column == columns.size() - 1) {
				output << "\t" << *((ROWID *)col.get() + i);
			} else {
				output << *((T *)col.get() + i) << "\t";
			}
		}
		output << endl;
	}
	cout << output.str() << endl << endl;
}

template <class ROWID, class T>
void PrintRows(data_ptr_t row_ptr, const idx_t &count, const vector<idx_t> &col_widths, const bool &row_id) {
	ostringstream output;
	for (idx_t i = 0; i < count; i++) {
		for (idx_t column = 0; column < col_widths.size(); column++) {
			if (row_id && column == col_widths.size() - 1) {
				output << "\t" << *((ROWID *)row_ptr);
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
			data_ptr_t source_ptr = columns[column].get() + i * col_width;
			data_ptr_t target_ptr = result.get() + i * row_width + row_offset;
			if (radix[column]) {
				for (idx_t j = 0; j < next; j++) {
					const T &val = *((T *)source_ptr);
					EncodeData<T>(target_ptr, val, is_little_endian);
					source_ptr += col_width;
					target_ptr += row_width;
				}
			} else {
				for (idx_t j = 0; j < next; j++) {
					memcpy(target_ptr, source_ptr, col_width);
					source_ptr += col_width;
					target_ptr += row_width;
				}
			}
			row_offset += col_width;
		}
		i += next;
	}
	return result;
}

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
			data_ptr_t target_ptr = result[column].get() + i * col_width;
			for (idx_t j = 0; j < next; j++) {
				memcpy(target_ptr, source_ptr, col_width);
				source_ptr += row_width;
				target_ptr += col_width;
			}
		}
		i += next;
	}
	return result;
}

unique_ptr<data_t[]> GatherRowID(unique_ptr<data_t[]> rows, const idx_t &count, const idx_t &row_width,
                                 const idx_t &col_width, const idx_t &offset) {
	auto result = unique_ptr<data_t[]>(new data_t[count * col_width]);
	data_ptr_t source_ptr = rows.get() + offset;
	data_ptr_t target_ptr = result.get();
	for (idx_t i = 0; i < count; i++) {
		memcpy(target_ptr, source_ptr, col_width);
		source_ptr += row_width;
		target_ptr += col_width;
	}
	return result;
}

//===--------------------------------------------------------------------===//
// ReOrder Simulation
//===--------------------------------------------------------------------===//
string CreateReOrderCSVHeader() {
	return "category,count,columns,col_width,total,reorder,scatter,gather";
}

vector<unique_ptr<data_t[]>> ReOrderColumns(const idx_t row_ids[], const vector<unique_ptr<data_t[]>> &columns,
                                            const idx_t &count, const idx_t &col_width) {
	auto result = AllocateColumns(count, columns.size(), col_width);
	for (idx_t column = 0; column < columns.size(); column++) {
		const data_ptr_t source_ptr = columns[column].get();
		data_ptr_t target_ptr = result[column].get();
		for (idx_t i = 0; i < count; i++) {
			memcpy(target_ptr, source_ptr + row_ids[i] * col_width, col_width);
			target_ptr += col_width;
		}
	}
	return result;
}

unique_ptr<data_t[]> ReOrderRows(const idx_t row_ids[], unique_ptr<data_t[]> rows, const idx_t &count,
                                 const idx_t &row_width) {
	auto result = AllocateRows(count, row_width);
	auto source_ptr = rows.get();
	auto target_ptr = result.get();
	for (idx_t i = 0; i < count; i++) {
		memcpy(target_ptr, source_ptr + row_ids[i] * row_width, row_width);
		target_ptr += row_width;
	}
	return result;
}

string SimulateColumnReOrder(const idx_t row_ids[], const idx_t &count, const idx_t &columns, const idx_t &col_width) {
	// Initialize source data
	auto source = AllocateColumns(count, columns, col_width);

	// ReOrder and timestamp
	auto before_timestamp = CurrentTime();
	auto target = ReOrderColumns(row_ids, source, count, col_width);
	auto after_timestamp = CurrentTime();

	// Compute duration of phases
	auto total_duration = after_timestamp - before_timestamp;
	return CreateOutput("col", {count, columns, col_width, total_duration, total_duration}, 8);
}

string SimulateRowReOrder(const idx_t row_ids[], const idx_t &count, const idx_t &columns, const idx_t &col_width) {
	// Initialize source data
	auto source = AllocateColumns(count, columns, col_width);

	// Scatter, ReOrder, Gather and timestamp
	auto before_timestamp = CurrentTime();
	idx_t row_width = 0;
	vector<idx_t> col_widths;
	vector<bool> radix;
	for (idx_t i = 0; i < columns; i++) {
		row_width += col_width;
		col_widths.push_back(col_width);
		radix.push_back(false);
	}

	auto source_rows = Scatter<idx_t>(move(source), count, row_width, col_widths, radix);
	auto scatter_timestamp = CurrentTime();
	auto target_rows = ReOrderRows(row_ids, move(source_rows), count, row_width);
	auto reorder_timestamp = CurrentTime();
	auto target = Gather(move(target_rows), count, row_width, col_widths);
	auto after_timestamp = CurrentTime();

	// Compute duration of phases
	auto total_duration = after_timestamp - before_timestamp;
	auto scatter_duration = scatter_timestamp - before_timestamp;
	auto reorder_duration = reorder_timestamp - scatter_timestamp;
	auto gather_duration = after_timestamp - reorder_timestamp;
	return CreateOutput(
	    "row", {count, columns, col_width, total_duration, reorder_duration, scatter_duration, gather_duration}, 8);
}

string SimulateReOrder(idx_t count, idx_t columns, idx_t col_width) {
	auto row_ids = InitRowIDs<idx_t>(count, true);
	ostringstream result;
	result << SimulateColumnReOrder((idx_t *)row_ids.get(), count, columns, col_width) << endl;
	result << SimulateRowReOrder((idx_t *)row_ids.get(), count, columns, col_width) << endl;
	return result.str();
}

void SimulateReOrder(idx_t row_max, idx_t col_max, idx_t col_width, idx_t iterations) {
	cout << "SimulateReOrder" << endl;
	ofstream results_file("results/reorder.csv", ios::trunc);
	results_file << CreateReOrderCSVHeader() << endl;
	for (idx_t r = 10; r < row_max; r += 2) {
		for (idx_t c = 0; c < col_max; c++) {
			for (idx_t i = 0; i < iterations; i++) {
				idx_t num_cols = min<idx_t>(1 << c, 96);
				results_file << SimulateReOrder(1 << r, num_cols, col_width);
				results_file.flush();
				cout << "." << flush;
			}
		}
		cout << endl;
	}
}

template <class T>
void VerifyRowReOrder(const idx_t row_ids[], idx_t count, idx_t columns) {
	// Initialize source data
	auto source = AllocateColumns(count, columns, sizeof(T));
	FillColumns<T>(source, count, true);
	cout << "--- BEFORE --- " << endl;
	PrintColumns<T, T>(source, count, columns);

	// Scatter, ReOrder, Gather, and print
	idx_t row_width = 0;
	vector<idx_t> col_widths;
	vector<bool> radix;
	for (idx_t i = 0; i < columns; i++) {
		row_width += sizeof(T);
		col_widths.push_back(sizeof(T));
		radix.push_back(false);
	}

	auto source_rows = Scatter<idx_t>(move(source), count, row_width, col_widths, radix);
	cout << "--- SCATTERED --- " << endl;
	PrintRows<T, T>(source_rows.get(), count, col_widths, false);

	auto target_rows = ReOrderRows(row_ids, move(source_rows), count, row_width);
	cout << "--- REORDERED --- " << endl;
	PrintRows<T, T>(target_rows.get(), count, col_widths, false);

	auto target = Gather(move(target_rows), count, row_width, col_widths);
	cout << "--- GATHERED --- " << endl;
	PrintColumns<T, T>(target, count, columns);
}

template <class T>
void VerifyColumnReOrder(const idx_t row_ids[], idx_t count, idx_t columns) {
	// Initialize source data
	auto source = AllocateColumns(count, columns, sizeof(T));
	FillColumns<T>(source, count, true);
	cout << "--- BEFORE --- " << endl;
	PrintColumns<T, T>(source, count, columns);

	// ReOrder and print
	auto target = ReOrderColumns(row_ids, source, count, sizeof(T));
	cout << "--- AFTER --- " << endl;
	PrintColumns<T, T>(target, count, columns);
}

void VerifyReOrder() {
	idx_t count = 5;
	idx_t columns = 5;

	auto row_ids = InitRowIDs<idx_t>(count, true);
	VerifyRowReOrder<idx_t>((idx_t *)row_ids.get(), count, columns);
	VerifyColumnReOrder<idx_t>((idx_t *)row_ids.get(), count, columns);
}

//===--------------------------------------------------------------------===//
// Comparator Simulation
//===--------------------------------------------------------------------===//
string CreateComparatorCSVHeader() {
	return "category,count,columns,row_id_width,col_width,total,sort,scatter,gather";
}

template <class ROWID, class T>
struct BranchlessRowOrderEntry1 {
public:
	T col1;
	ROWID row_id;

public:
	bool operator<(const BranchlessRowOrderEntry1 &rhs) const {
		return memcmp(&col1, &rhs.col1, sizeof(T)) < 0;
	}
};

template <class ROWID, class T>
struct BranchlessRowOrderEntry2 {
public:
	T col1;
	T col2;
	ROWID row_id;

public:
	bool operator<(const BranchlessRowOrderEntry2 &rhs) const {
		return memcmp(&col1, &rhs.col1, 2 * sizeof(T)) < 0;
	}
};

template <class ROWID, class T>
struct BranchlessRowOrderEntry3 {
public:
	T col1;
	T col2;
	T col3;
	ROWID row_id;

public:
	bool operator<(const BranchlessRowOrderEntry3 &rhs) const {
		return memcmp(&col1, &rhs.col1, 3 * sizeof(T)) < 0;
	}
};

template <class ROWID, class T>
struct BranchlessRowOrderEntry4 {
public:
	T col1;
	T col2;
	T col3;
	T col4;
	ROWID row_id;

public:
	bool operator<(const BranchlessRowOrderEntry4 &rhs) const {
		return memcmp(&col1, &rhs.col1, 4 * sizeof(T)) < 0;
	}
};

template <class ROWID, class T>
struct BranchlessRowOrderEntry5 {
public:
	T col1;
	T col2;
	T col3;
	T col4;
	T col5;
	ROWID row_id;

public:
	bool operator<(const BranchlessRowOrderEntry5 &rhs) const {
		return memcmp(&col1, &rhs.col1, 5 * sizeof(T)) < 0;
	}
};

template <class ROWID, class T>
struct BranchlessRowOrderEntry6 {
public:
	T col1;
	T col2;
	T col3;
	T col4;
	T col5;
	T col6;
	ROWID row_id;

public:
	bool operator<(const BranchlessRowOrderEntry6 &rhs) const {
		return memcmp(&col1, &rhs.col1, 6 * sizeof(T)) < 0;
	}
};

template <class ROWID, class T>
struct BranchlessRowOrderEntry7 {
public:
	T col1;
	T col2;
	T col3;
	T col4;
	T col5;
	T col6;
	T col7;
	ROWID row_id;

public:
	bool operator<(const BranchlessRowOrderEntry7 &rhs) const {
		return memcmp(&col1, &rhs.col1, 7 * sizeof(T)) < 0;
	}
};

template <class ROWID, class T>
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
	ROWID row_id;

public:
	bool operator<(const BranchlessRowOrderEntry8 &rhs) const {
		return memcmp(&col1, &rhs.col1, 8 * sizeof(T)) < 0;
	}
};

template <class ROWID, class T>
struct BranchedRowOrderEntry1 {
public:
	T col1;
	ROWID row_id;

public:
	bool operator<(const BranchedRowOrderEntry1 &rhs) const {
		return col1 < rhs.col1;
	}
};

template <class ROWID, class T>
struct BranchedRowOrderEntry2 {
public:
	T col1;
	T col2;
	ROWID row_id;

public:
	bool operator<(const BranchedRowOrderEntry2 &rhs) const {
		const T *order_columns = &col1;
		const T *rhs_order_columns = &rhs.col1;
		for (idx_t i = 0; i < 2; i++) {
			if (order_columns[i] != rhs_order_columns[i]) {
				return order_columns[i] < rhs_order_columns[i];
			}
		}
		return false;
	}
};

template <class ROWID, class T>
struct BranchedRowOrderEntry3 {
public:
	T col1;
	T col2;
	T col3;
	ROWID row_id;

public:
	bool operator<(const BranchedRowOrderEntry3 &rhs) const {
		const T *order_columns = &col1;
		const T *rhs_order_columns = &rhs.col1;
		for (idx_t i = 0; i < 3; i++) {
			if (order_columns[i] != rhs_order_columns[i]) {
				return order_columns[i] < rhs_order_columns[i];
			}
		}
		return false;
	}
};

template <class ROWID, class T>
struct BranchedRowOrderEntry4 {
public:
	T col1;
	T col2;
	T col3;
	T col4;
	ROWID row_id;

public:
	bool operator<(const BranchedRowOrderEntry4 &rhs) const {
		const T *order_columns = &col1;
		const T *rhs_order_columns = &rhs.col1;
		for (idx_t i = 0; i < 4; i++) {
			if (order_columns[i] != rhs_order_columns[i]) {
				return order_columns[i] < rhs_order_columns[i];
			}
		}
		return false;
	}
};

template <class ROWID, class T>
struct BranchedRowOrderEntry5 {
public:
	T col1;
	T col2;
	T col3;
	T col4;
	T col5;
	ROWID row_id;

public:
	bool operator<(const BranchedRowOrderEntry5 &rhs) const {
		const T *order_columns = &col1;
		const T *rhs_order_columns = &rhs.col1;
		for (idx_t i = 0; i < 5; i++) {
			if (order_columns[i] != rhs_order_columns[i]) {
				return order_columns[i] < rhs_order_columns[i];
			}
		}
		return false;
	}
};

template <class ROWID, class T>
struct BranchedRowOrderEntry6 {
public:
	T col1;
	T col2;
	T col3;
	T col4;
	T col5;
	T col6;
	ROWID row_id;

public:
	bool operator<(const BranchedRowOrderEntry6 &rhs) const {
		const T *order_columns = &col1;
		const T *rhs_order_columns = &rhs.col1;
		for (idx_t i = 0; i < 6; i++) {
			if (order_columns[i] != rhs_order_columns[i]) {
				return order_columns[i] < rhs_order_columns[i];
			}
		}
		return false;
	}
};

template <class ROWID, class T>
struct BranchedRowOrderEntry7 {
public:
	T col1;
	T col2;
	T col3;
	T col4;
	T col5;
	T col6;
	T col7;
	ROWID row_id;

public:
	bool operator<(const BranchedRowOrderEntry7 &rhs) const {
		const T *order_columns = &col1;
		const T *rhs_order_columns = &rhs.col1;
		for (idx_t i = 0; i < 7; i++) {
			if (order_columns[i] != rhs_order_columns[i]) {
				return order_columns[i] < rhs_order_columns[i];
			}
		}
		return false;
	}
};

template <class ROWID, class T>
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
	ROWID row_id;

public:
	bool operator<(const BranchedRowOrderEntry8 &rhs) const {
		const T *order_columns = &col1;
		const T *rhs_order_columns = &rhs.col1;
		for (idx_t i = 0; i < 8; i++) {
			if (order_columns[i] != rhs_order_columns[i]) {
				return order_columns[i] < rhs_order_columns[i];
			}
		}
		return false;
	}
};

template <class ROWID, class T>
void SortRowBranchless(data_ptr_t row_data, const idx_t &count, const idx_t &columns) {
	switch (columns) {
	case 1: {
		auto row_data_ptr = (BranchlessRowOrderEntry1<ROWID, T> *)row_data;
		sort(row_data_ptr, row_data_ptr + count);
		break;
	}
	case 2: {
		auto row_data_ptr = (BranchlessRowOrderEntry2<ROWID, T> *)row_data;
		sort(row_data_ptr, row_data_ptr + count);
		break;
	}
	case 3: {
		auto row_data_ptr = (BranchlessRowOrderEntry3<ROWID, T> *)row_data;
		sort(row_data_ptr, row_data_ptr + count);
		break;
	}
	case 4: {
		auto row_data_ptr = (BranchlessRowOrderEntry4<ROWID, T> *)row_data;
		sort(row_data_ptr, row_data_ptr + count);
		break;
	}
	case 5: {
		auto row_data_ptr = (BranchlessRowOrderEntry5<ROWID, T> *)row_data;
		sort(row_data_ptr, row_data_ptr + count);
		break;
	}
	case 6: {
		auto row_data_ptr = (BranchlessRowOrderEntry6<ROWID, T> *)row_data;
		sort(row_data_ptr, row_data_ptr + count);
		break;
	}
	case 7: {
		auto row_data_ptr = (BranchlessRowOrderEntry7<ROWID, T> *)row_data;
		sort(row_data_ptr, row_data_ptr + count);
		break;
	}
	case 8: {
		auto row_data_ptr = (BranchlessRowOrderEntry8<ROWID, T> *)row_data;
		sort(row_data_ptr, row_data_ptr + count);
		break;
	}
	default:
		throw exception();
	}
}

template <class ROWID, class T>
void SortRowBranched(data_ptr_t row_data, const idx_t &count, const idx_t &columns) {
	switch (columns) {
	case 1: {
		auto row_data_ptr = (BranchedRowOrderEntry1<ROWID, T> *)row_data;
		sort(row_data_ptr, row_data_ptr + count);
		break;
	}
	case 2: {
		auto row_data_ptr = (BranchedRowOrderEntry2<ROWID, T> *)row_data;
		sort(row_data_ptr, row_data_ptr + count);
		break;
	}
	case 3: {
		auto row_data_ptr = (BranchedRowOrderEntry3<ROWID, T> *)row_data;
		sort(row_data_ptr, row_data_ptr + count);
		break;
	}
	case 4: {
		auto row_data_ptr = (BranchedRowOrderEntry4<ROWID, T> *)row_data;
		sort(row_data_ptr, row_data_ptr + count);
		break;
	}
	case 5: {
		auto row_data_ptr = (BranchedRowOrderEntry5<ROWID, T> *)row_data;
		sort(row_data_ptr, row_data_ptr + count);
		break;
	}
	case 6: {
		auto row_data_ptr = (BranchedRowOrderEntry6<ROWID, T> *)row_data;
		sort(row_data_ptr, row_data_ptr + count);
		break;
	}
	case 7: {
		auto row_data_ptr = (BranchedRowOrderEntry7<ROWID, T> *)row_data;
		sort(row_data_ptr, row_data_ptr + count);
		break;
	}
	case 8: {
		auto row_data_ptr = (BranchedRowOrderEntry8<ROWID, T> *)row_data;
		sort(row_data_ptr, row_data_ptr + count);
		break;
	}
	default:
		throw exception();
	}
}

template <class ROWID, class T>
void SortColumn(unique_ptr<data_t[]> &row_id_col, vector<unique_ptr<data_t[]>> &columns, const idx_t &count) {
	ROWID *row_ids = (ROWID *)row_id_col.get();
	vector<T *> typed_columns;
	for (const auto &col : columns) {
		typed_columns.push_back((T *)col.get());
	}
	if (columns.size() == 0) {
		const auto &col = typed_columns[0];
		sort(row_ids, row_ids + count,
		     [&row_ids, &col](const ROWID &lhs, const ROWID &rhs) -> bool { return col[lhs] < col[rhs]; });
	} else {
		const auto &cols = typed_columns;
		sort(row_ids, row_ids + count, [&row_ids, &cols](const ROWID &lhs, const ROWID &rhs) -> bool {
			for (const auto &col : cols) {
				if (col[lhs] != col[rhs]) {
					return col[lhs] < col[rhs];
				}
			}
			return false;
		});
	}
}

template <class ROWID, class T>
string SimulateRowComparator(const idx_t &count, const idx_t &columns, bool branchless) {
	// This must hold for for alignment
	assert((columns * sizeof(T)) % sizeof(ROWID) == 0);

	// Initialize source data
	auto row_ids = InitRowIDs<ROWID>(count, false);
	auto source = AllocateColumns(count, columns, sizeof(T));
	FillColumns<T>(source, count, false);
	source.push_back(move(row_ids));
	// PrintColumns<ROWID, T>(source, count, columns);

	auto before_timestamp = CurrentTime();
	idx_t row_width = 0;
	vector<idx_t> col_widths;
	vector<bool> radix;
	for (idx_t i = 0; i < columns; i++) {
		row_width += sizeof(T);
		col_widths.push_back(sizeof(T));
		radix.push_back(branchless);
	}
	row_width += sizeof(ROWID);
	col_widths.push_back(sizeof(ROWID));
	radix.push_back(false);

	auto row_data = Scatter<T>(move(source), count, row_width, col_widths, radix);
	auto scatter_timestamp = CurrentTime();
	// PrintRows<ROWID, T>(row_data.get(), count, col_widths, true);

	if (branchless) {
		SortRowBranchless<ROWID, T>(row_data.get(), count, columns);
	} else {
		SortRowBranched<ROWID, T>(row_data.get(), count, columns);
	}
	auto sort_timestamp = CurrentTime();
	// PrintRows<ROWID, T>(row_data.get(), count, col_widths, true);

	auto target = GatherRowID(move(row_data), count, row_width, sizeof(ROWID), row_width - sizeof(ROWID));
	auto after_timestamp = CurrentTime();

	// Verification stuff
	// vector<unique_ptr<data_t[]>> dummy_cols;
	// dummy_cols.push_back(move(target));
	// PrintColumns<ROWID, T>(dummy_cols, count, 0);

	// Compute duration of phases
	auto total_duration = after_timestamp - before_timestamp;
	auto scatter_duration = scatter_timestamp - before_timestamp;
	auto sort_duration = sort_timestamp - scatter_timestamp;
	auto gather_duration = after_timestamp - sort_timestamp;
	string category = branchless ? "row_branchless" : "row_branched";
	return CreateOutput(
	    category,
	    {count, columns, sizeof(ROWID), sizeof(T), total_duration, sort_duration, scatter_duration, gather_duration},
	    9);
}

template <class ROWID, class T>
string SimulateColumnComparator(const idx_t &count, const idx_t &columns) {
	// Initialize source data
	auto row_ids = InitRowIDs<ROWID>(count, false);
	auto source = AllocateColumns(count, columns, sizeof(T));
	FillColumns<T>(source, count, false);

	auto before_timestamp = CurrentTime();
	SortColumn<ROWID, T>(row_ids, source, count);
	auto after_timestamp = CurrentTime();

	vector<unique_ptr<data_t[]>> dummy_cols;
	dummy_cols.push_back(move(row_ids));
	// PrintColumns<ROWID, T>(dummy_cols, count, 0);

	// Compute duration of phases
	auto total_duration = after_timestamp - before_timestamp;
	return CreateOutput("col", {count, columns, sizeof(ROWID), sizeof(T), total_duration, total_duration}, 9);
}

template <class ROWID, class T>
string SimulateComparator(idx_t count, idx_t columns) {
	ostringstream result;
	result << SimulateColumnComparator<ROWID, T>(count, columns) << endl;
	result << SimulateRowComparator<ROWID, T>(count, columns, true) << endl;
	result << SimulateRowComparator<ROWID, T>(count, columns, false) << endl;
	return result.str();
}

void SimulateComparator(idx_t row_max, idx_t col_max, idx_t iterations) {
	cout << "SimulateComparator" << endl;
	ofstream results_file("results/comparator.csv", ios::trunc);
	results_file << CreateComparatorCSVHeader() << endl;
	for (idx_t r = 10; r < row_max; r += 2) {
		for (idx_t c = 1; c < col_max + 1; c++) {
			for (idx_t i = 0; i < iterations; i++) {
				results_file << SimulateComparator<uint32_t, uint32_t>(1 << r, c);
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
	return "category,count,columns,row_id_width,col_width,total,sort,scatter,gather";
}

void RadixSort(data_ptr_t orig_ptr, const idx_t &count, const idx_t &row_width, const idx_t &comp_width) {
	if (count <= 1) {
		return;
	}
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
		for (idx_t val = 1; val < 256; val++) {
			counts[val] = counts[val] + counts[val - 1];
		}
		// Re-order the data in temporary array
		data_ptr_t row_ptr = source_ptr + (count - 1) * row_width;
		for (idx_t i = 0; i < count; i++) {
			idx_t &radix_offset = --counts[*(row_ptr + offset)];
			memcpy(target_ptr + radix_offset * row_width, row_ptr, row_width);
			row_ptr -= row_width;
		}
		swap = !swap;
	}
	// Move data back to original buffer (if it was swapped)
	if (swap) {
		memcpy(orig_ptr, temp_block.get(), count * row_width);
	}
}

template <class ROWID, class T>
string SimulateSort(const idx_t &count, const idx_t &columns, bool radix_sort) {
	// This must hold for for alignment
	assert((columns * sizeof(T)) % sizeof(ROWID) == 0);

	// Initialize source data
	auto row_ids = InitRowIDs<ROWID>(count, false);
	auto source = AllocateColumns(count, columns, sizeof(T));
	FillColumns<T>(source, count, false);
	source.push_back(move(row_ids));
	// PrintColumns<ROWID, T>(source, count, columns);

	auto before_timestamp = CurrentTime();
	idx_t row_width = 0;
	vector<idx_t> col_widths;
	vector<bool> radix;
	for (idx_t i = 0; i < columns; i++) {
		row_width += sizeof(T);
		col_widths.push_back(sizeof(T));
		radix.push_back(true);
	}
	row_width += sizeof(ROWID);
	col_widths.push_back(sizeof(ROWID));
	radix.push_back(false);

	auto row_data = Scatter<T>(move(source), count, row_width, col_widths, radix);
	auto scatter_timestamp = CurrentTime();
	// PrintRows<ROWID, T>(row_data.get(), count, col_widths, true);

	if (radix_sort) {
		RadixSort(row_data.get(), count, row_width, row_width - sizeof(ROWID));
	} else {
		SortRowBranchless<ROWID, T>(row_data.get(), count, columns);
	}
	auto sort_timestamp = CurrentTime();
	// PrintRows<ROWID, T>(row_data.get(), count, col_widths, true);

	auto target = GatherRowID(move(row_data), count, row_width, sizeof(ROWID), row_width - sizeof(ROWID));
	auto after_timestamp = CurrentTime();

	// Verification stuff
	// vector<unique_ptr<data_t[]>> dummy_cols;
	// dummy_cols.push_back(move(target));
	// PrintColumns<ROWID, T>(dummy_cols, count, 0);

	// Compute duration of phases
	auto total_duration = after_timestamp - before_timestamp;
	auto scatter_duration = scatter_timestamp - before_timestamp;
	auto sort_duration = sort_timestamp - scatter_timestamp;
	auto gather_duration = after_timestamp - sort_timestamp;
	string category = radix_sort ? "rad" : "std";
	return CreateOutput(
	    category,
	    {count, columns, sizeof(ROWID), sizeof(T), total_duration, sort_duration, scatter_duration, gather_duration},
	    9);
}

template <class ROWID, class T>
string SimulateSort(idx_t count, idx_t columns) {
	ostringstream result;
	result << SimulateSort<ROWID, T>(count, columns, true) << endl;
	result << SimulateSort<ROWID, T>(count, columns, false) << endl;
	return result.str();
}

void SimulateSort(idx_t row_max, idx_t col_max, idx_t iterations) {
	cout << "SimulateSort" << endl;
	ofstream results_file("results/sort.csv", ios::trunc);
	results_file << CreateSortCSVHeader() << endl;
	for (idx_t r = 10; r < row_max; r += 2) {
		for (idx_t c = 1; c < col_max + 1; c++) {
			for (idx_t i = 0; i < iterations; i++) {
				results_file << SimulateSort<uint32_t, uint32_t>(1 << r, c);
				results_file.flush();
				cout << "." << flush;
			}
		}
		cout << endl;
	}
}

//===--------------------------------------------------------------------===//
// Merge Simulation
//===--------------------------------------------------------------------===//
string CreateMergeCSVHeader() {
	return "category,count,columns,col_width,total";
}

vector<unique_ptr<data_t[]>> MergeColumns(vector<unique_ptr<data_t[]>> left, vector<unique_ptr<data_t[]>> right,
                                          const idx_t &count, const idx_t &col_width) {
	auto result = AllocateColumns(count * 2, left.size(), col_width);
	bool left_smaller[STANDARD_VECTOR_SIZE];
	idx_t l_i = 0;
	idx_t r_i = 0;
	idx_t result_i = 0;
	while (l_i != count || r_i != count) {
		if (l_i == count) {
			idx_t entries = count - r_i;
			for (idx_t column = 0; column < right.size(); column++) {
				data_ptr_t r_ptr = right[column].get() + r_i * col_width;
				data_ptr_t target_ptr = result[column].get() + result_i * col_width;
				memcpy(target_ptr, r_ptr, entries * col_width);
			}
			result_i += entries;
			r_i += entries;
		} else if (r_i == count) {
			idx_t entries = count - l_i;
			for (idx_t column = 0; column < left.size(); column++) {
				data_ptr_t l_ptr = left[column].get() + l_i * col_width;
				data_ptr_t target_ptr = result[column].get() + result_i * col_width;
				memcpy(target_ptr, l_ptr, entries * col_width);
			}
			result_i += entries;
			l_i += entries;
		} else {
			idx_t l_i_temp = l_i;
			idx_t r_i_temp = r_i;
			idx_t j;
			for (j = 0; j < STANDARD_VECTOR_SIZE && l_i_temp < count && r_i_temp < count; j++) {
				left_smaller[j] = ((double)rand() / (RAND_MAX)) < 0.5;
				l_i_temp += left_smaller[j];
				r_i_temp += !left_smaller[j];
			}
			const idx_t next = j;
			for (idx_t column = 0; column < left.size(); column++) {
				data_ptr_t l_ptr = left[column].get() + l_i * col_width;
				data_ptr_t r_ptr = right[column].get() + r_i * col_width;
				data_ptr_t target_ptr = result[column].get() + result_i * col_width;
				for (j = 0; j < next; j++) {
					bool &copy_left = left_smaller[j];
					bool copy_right = !copy_left;
					memcpy(target_ptr, l_ptr, copy_left * col_width);
					memcpy(target_ptr, r_ptr, copy_right * col_width);
					target_ptr += col_width;
					l_ptr += copy_left * col_width;
					r_ptr += copy_right * col_width;
				}
			}
			result_i += next;
			l_i = l_i_temp;
			r_i = r_i_temp;
		}
	}
	return result;
}

unique_ptr<data_t[]> MergeRows(unique_ptr<data_t[]> left, unique_ptr<data_t[]> right, const idx_t &count,
                               const idx_t &row_width) {
	auto result = AllocateRows(count * 2, row_width);
	bool left_smaller[STANDARD_VECTOR_SIZE];
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
				left_smaller[j] = ((double)rand() / (RAND_MAX)) < 0.5;
				l_i_temp += left_smaller[j];
				r_i_temp += !left_smaller[j];
			}
			const idx_t next = j;
			data_ptr_t l_ptr = left.get() + l_i * row_width;
			data_ptr_t r_ptr = right.get() + r_i * row_width;
			data_ptr_t target_ptr = result.get() + result_i * row_width;
			for (j = 0; j < next; j++) {
				bool &copy_left = left_smaller[j];
				bool copy_right = !copy_left;
				memcpy(target_ptr, l_ptr, copy_left * row_width);
				memcpy(target_ptr, r_ptr, copy_right * row_width);
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

string SimulateColumnMerge(const idx_t &count, const idx_t &columns, const idx_t &col_width) {
	// Initialize source data
	auto left = AllocateColumns(count / 2, columns, col_width);
	auto right = AllocateColumns(count / 2, columns, col_width);

	// Merge
	auto before_timestamp = CurrentTime();
	auto target = MergeColumns(move(left), move(right), count / 2, col_width);
	auto after_timestamp = CurrentTime();

	// Compute duration of phases
	auto total_duration = after_timestamp - before_timestamp;
	return CreateOutput("col", {count, columns, col_width, total_duration}, 5);
}

string SimulateRowMerge(const idx_t &count, const idx_t &columns, const idx_t &col_width) {
	// Initialize source data
	const idx_t row_width = columns * col_width;
	auto left = AllocateRows(count / 2, row_width);
	auto right = AllocateRows(count / 2, row_width);

	// Merge
	auto before_timestamp = CurrentTime();
	auto target = MergeRows(move(left), move(right), count / 2, row_width);
	auto after_timestamp = CurrentTime();

	// Compute duration of phases
	auto total_duration = after_timestamp - before_timestamp;
	return CreateOutput("row", {count, columns, col_width, total_duration}, 5);
}

string SimulateMerge(idx_t count, idx_t columns, idx_t col_width) {
	ostringstream result;
	result << SimulateColumnMerge(count, columns, col_width) << endl;
	result << SimulateRowMerge(count, columns, col_width) << endl;
	return result.str();
}

void SimulateMerge(idx_t row_max, idx_t col_max, idx_t col_width, idx_t iterations) {
	cout << "SimulateMerge" << endl;
	ofstream results_file("results/merge.csv", ios::trunc);
	results_file << CreateMergeCSVHeader() << endl;
	for (idx_t r = 4; r < row_max; r += 2) {
		for (idx_t c = 0; c < col_max; c++) {
			for (idx_t i = 0; i < iterations; i++) {
				idx_t num_cols = min<idx_t>(1 << c, 96);
				results_file << SimulateMerge(1 << r, num_cols, col_width);
				results_file.flush();
				cout << "." << flush;
			}
		}
		cout << endl;
	}
}

//===--------------------------------------------------------------------===//
// Main
//===--------------------------------------------------------------------===//
int main(int argc, char *argv[]) {
	// NOTE: for comparator we'd like a lot of key collisions so that the 2nd, 3rd, etc. columns are needed more often
	//  This would allow us to show off the better data locality of the row comparator
	//  However, radix sort is definitely worse on this same data: No such thing as free lunch!
	if (argc == 1) {
		// VerifyReOrder();
		SimulateReOrder(25, 8, 4, 3);
		SimulateComparator(25, 8, 3);
		SimulateSort(25, 8, 3);
		SimulateMerge(25, 8, 4, 3);
	} else {
		assert(argc == 5);
		auto category = string(argv[2]);
		assert(category == "col" || category == "row");
		auto sim = string(argv[1]);
		assert(sim == "reorder" || sim == "comparator" || sim == "sort" || sim == "merge");
		int count = 1 << stoi(argv[3]);
		int columns = stoi(argv[4]);
		cout << sim << " " << category << " " << count << " " << columns << endl;
		if (category == "col") {
			if (sim == "reorder") {
				auto row_ids = InitRowIDs<idx_t>(count, true);
				SimulateColumnReOrder((idx_t *)row_ids.get(), count, columns, 4);
			} else if (sim == "comparator") {
				SimulateColumnComparator<uint32_t, uint32_t>(count, columns);
			} else if (sim == "sort") {
				// category "col" means std::sort here
				SimulateSort<uint32_t, uint32_t>(count, columns, false);
			} else if (sim == "merge") {
				SimulateColumnMerge(count, columns, 4);
			}
		} else if (category == "row") {
			if (sim == "reorder") {
				auto row_ids = InitRowIDs<idx_t>(count, true);
				SimulateRowReOrder((idx_t *)row_ids.get(), count, columns, 4);
			} else if (sim == "comparator") {
				SimulateRowComparator<uint32_t, uint32_t>(count, columns, true);
			} else if (sim == "sort") {
				// category "col" means radix sort here
				SimulateSort<uint32_t, uint32_t>(count, columns, true);
			} else if (sim == "merge") {
				SimulateRowMerge(count, columns, 4);
			}
		}
	}
}
