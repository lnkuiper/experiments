import os
import sys
from threading import Thread


SYSTEM_DIR = os.path.dirname(__file__)
sys.path.append(f'{SYSTEM_DIR}/..')
from util.util import *


THREAD_COUNT = 1
EVICTION_POLICY_REPETITIONS = 10
POLLING_INTERVAL = 1


def run_query_loop(con):
    cur = con.cursor()
    for _ in range(EVICTION_POLICY_REPETITIONS):
        cur.sql("""SELECT l_orderkey FROM lineitem100 GROUP BY l_orderkey OFFSET 149999999""").fetchall()


def main():
    db_path = f'{SYSTEM_DIR}/data.db'
    # db_path = '/data/data.db'
    con = duckdb.connect(db_path, read_only=True)
    con.execute("""SET preserve_insertion_order=false""")
    con.execute(f"""SET memory_limit='{3 * THREAD_COUNT}GB'""") # 4 GB per concurrent reader
    con.execute(f"""SET threads={4 * THREAD_COUNT}""") # 4 threads per concurrent reader

    eviction_policy_db_path = f'{SYSTEM_DIR}/eviction_policy.db'
    if os.path.exists(eviction_policy_db_path):
        os.remove(eviction_policy_db_path)

    results_con = con.cursor()
    results_con.execute(f"""ATTACH IF NOT EXISTS '{eviction_policy_db_path}' AS results (READ_ONLY FALSE)""")
    results_con.execute("""CREATE OR REPLACE TABLE results.results (time DOUBLE, tag VARCHAR, memory_usage_bytes BIGINT, temporary_storage_bytes BIGINT)""")

    threads = []
    for i in range(THREAD_COUNT):
        threads.append(Thread(target=run_query_loop, args=(con,)))
    
    before = time.time()
    for thread in threads:
        thread.start()
    while threads[0].is_alive():
        results_con.execute(f"""INSERT INTO results.results SELECT {time.time() - before}, * FROM duckdb_memory() WHERE tag = 'BASE_TABLE' OR tag = 'HASH_TABLE'""")
        time.sleep(POLLING_INTERVAL)
    for thread in threads:
        thread.join()
    runtime = time.time() - before

    print(f'TOTAL RUNTIME: {runtime}s')


if __name__ == '__main__':
    main()
