import os
import sys


SYSTEM_DIR = os.path.dirname(__file__)
sys.path.append(f'{SYSTEM_DIR}/..')
from util.util import *


def run_query(con, config, threads):
	before = time.time()
	con.execute(f"""
		SET threads={threads};
		SELECT count(*)
		FROM (SELECT DISTINCT *
		      FROM "{conf_to_str(config)}")
	""").fetchall()
	return time.time() - before


def insert_result(con, config, threads, time):
	con.execute(f"""
		INSERT INTO {RESULTS_TABLE_NAME} VALUES
		({config['total_count']}, '{config['type']}', {config['column_count']}, {config['power']}, {config['group_count']}, {threads}, {time})
	""")


def benchmark_groups(results_con, benchmark_con, config):
    for threads in [1, 2, 4, 8]:
    	repetitions = get_repetition_count(results_con, config, threads)
    	for _ in range(repetitions):
    		time = run_query(benchmark_con, config, threads)
    		insert_result(results_con, config, threads, time)


def main():
    results_con = get_results_con('duckdb')
    results_con.execute(f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{RESULTS_TABLE_NAME}'")
    if results_con.fetchall()[0][0] == 0:
        results_con.execute(f"CREATE TABLE {RESULTS_TABLE_NAME} ({', '.join(RESULTS_TABLE_COLS)})")

    config = load_config(results_con)
    configs = permute_dicts(config)

    benchmark_con = duckdb.connect(f'{SYSTEM_DIR}/data.db', read_only=True)
    print('Running DuckDB ...')
    for c in tqdm.tqdm(configs):
        benchmark_groups(results_con, benchmark_con, c)
    print('Done.')


if __name__ == '__main__':
    main()
