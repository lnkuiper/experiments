import os
import sys
from threading import Thread


SYSTEM_DIR = os.path.dirname(__file__)
sys.path.append(f'{SYSTEM_DIR}/..')
from util.util import *


THREAD_COUNTS = [1, 4]
EVICTION_POLICY_REPETITIONS = 10
POLLING_INTERVAL = 0.5
POLICY = 'all'
#POLICY = 'persistent_temporary'
#POLICY = 'temporary_persistent'


def run_query_loop(con):
    cur = con.cursor()
    for _ in range(EVICTION_POLICY_REPETITIONS):
        cur.sql("""SELECT count(*) FROM (SELECT distinct(l_orderkey) FROM lineitem128)""").fetchall()


def main():
    for THREAD_COUNT in THREAD_COUNTS:
        #db_path = f'{SYSTEM_DIR}/data.db'
        db_path = '/data/data.db'
        con = duckdb.connect(db_path, read_only=True)
        con.execute("""SET preserve_insertion_order=false""")
        con.execute(f"""SET memory_limit='{5 * THREAD_COUNT}GB'""") # 3.5 GB per concurrent reader
        con.execute(f"""SET threads={4 * THREAD_COUNT}""") # 4 threads per concurrent reader
    
        eviction_policy_db_path = f'{SYSTEM_DIR}/eviction_policy.db'
        results_con = con.cursor()
        results_con.execute(f"""ATTACH IF NOT EXISTS '{eviction_policy_db_path}' AS results (READ_ONLY FALSE)""")
        results_con.execute("""CREATE TABLE IF NOT EXISTS results.results (policy VARCHAR, time DOUBLE, threads UTINYINT, tag VARCHAR, memory_usage_bytes BIGINT, temporary_storage_bytes BIGINT)""")
    
        threads = []
        for i in range(THREAD_COUNT):
            threads.append(Thread(target=run_query_loop, args=(con,)))
        
        before = time.time()
        for thread in threads:
            thread.start()
        while True:
            results_con.execute(f"""INSERT INTO results.results SELECT '{POLICY}', {time.time() - before}, {THREAD_COUNT}, * FROM duckdb_memory() WHERE tag = 'BASE_TABLE' OR tag = 'HASH_TABLE'""")
            at_least_one_alive = False
            for thread in threads:
                if thread.is_alive():
                    at_least_one_alive = True
                    break
            if not at_least_one_alive:
                break
            time.sleep(POLLING_INTERVAL)
        for thread in threads:
            thread.join()


if __name__ == '__main__':
    main()
