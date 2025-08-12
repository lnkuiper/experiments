import copy
import duckdb
import os
import time
import tqdm
from timeout_decorator import timeout, TimeoutError

BASE_DIR = f'{os.path.dirname(__file__)}/../..'

QUERIES_DIR = f'{BASE_DIR}/queries'
DATA_DIR = f'{BASE_DIR}/data'
RESULTS_DIR = f'{BASE_DIR}/results'

SCALE_FACTORS = [1, 10, 100]

REPETITIONS = 5
RESULTS_TABLE_NAME = 'results'
RESULTS_TABLE_COLS = [
    'sf USMALLINT',
    'q UTINYINT',
    't DOUBLE',
]


def get_schema(sf):
    with open(f'{DATA_DIR}/sf{sf}/schema.sql') as f:
        return f.read()


def get_load(sf):
    with open(f'{DATA_DIR}/sf{sf}/load.sql') as f:
        return f.read()


def get_queries():
    queries = []
    for file_name in os.listdir(QUERIES_DIR):
        file_path = f'{QUERIES_DIR}/{file_name}'
        with open(file_path, 'r') as f:
            queries.append((int(file_name.split('.')[0]), f.read()))
    return sorted(queries)


def get_results_con(name):
    if not os.path.exists(RESULTS_DIR):
        os.mkdir(RESULTS_DIR)
    con = duckdb.connect(f'{RESULTS_DIR}/{name}.duckdb')
    con.execute(f"CREATE TABLE IF NOT EXISTS {RESULTS_TABLE_NAME} ({', '.join(RESULTS_TABLE_COLS)});")
    return con


def get_repetition_count(con, name, sf, q):
    repetitions = con.execute(f"SELECT count(*) FROM {RESULTS_TABLE_NAME} WHERE sf = {sf} AND q = {q};").fetchall()[0][0]
    return REPETITIONS - repetitions


def insert_result(con, name, sf, q, t):
    con.execute(f"INSERT INTO {RESULTS_TABLE_NAME} VALUES ({sf}, {q}, {t});")


@timeout(1000)
def timeout_fun(fun, query, *args):
    before = time.time()
    res = fun(query, *args)
    t = time.time() - before
    return t, res


def run_query(name, result_con, sf, q, query, query_fun, close_fun, *args):
    repetitions = get_repetition_count(result_con, name, sf, q)
    error = 1
    for _ in range(repetitions):
        t = 0
        res = None
        if error < 0:
            t = error
        else:
            try:
                t, res = timeout_fun(query_fun, query, *args)
            except TimeoutError:
                t = -1
            except Exception as e:
                raise e
                t = -2
            finally:
                close_fun(res)
            error = t
        insert_result(result_con, name, sf, q, t)


def run_benchmark(name, schema_fun, query_fun, close_fun, *args):
    duckdb.sql("SET threads=1;")
    result_con = get_results_con(name)
    result_con.execute("SET threads=1;")
    result_con.execute("SET memory_limit='50mb';")
    for sf in SCALE_FACTORS:
        schema_fun(sf, *args)
        queries = get_queries()
        print(f'Running {name} SF{sf} ...')
        for q, query in tqdm.tqdm(queries):
            run_query(name, result_con, sf, q, query, query_fun, close_fun, *args)
        print(f'Running {name} SF{sf} done.')
