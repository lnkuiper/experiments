import duckdb
import time


def init(con, sf=1):
	con.execute(f"CALL dbgen(sf={sf})")


def get_queries():
	queries = []
	for i in range(1, 23):
		queries.append((i, f"PRAGMA tpch({i})"))
	return queries


def run_benchmark(con):
	for qnum, query in get_queries():
		for i in range(5):
			before = time.time()
			con.execute(query)
			after = time.time()
			print(f"{qnum}\t{after - before}")


def main():
	con = duckdb.connect()
	init(con)
	run_benchmark(con)
	con.execute("PRAGMA debug_force_external=true")
	run_benchmark(con)


if __name__ == "__main__":
	main()
