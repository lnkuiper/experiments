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

# SCALE_FACTORS = [32, 64, 128, 256]
SCALE_FACTORS = [1, 2]

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
    return queries


def get_results_con(name):
    if not os.path.exists(RESULTS_DIR):
        os.mkdir(RESULTS_DIR)
    con = duckdb.connect(f'{RESULTS_DIR}/{name}.duckdb')
    con.execute(f"CREATE TABLE IF NOT EXISTS {RESULTS_TABLE_NAME} ({', '.join(RESULTS_TABLE_COLS)});")
    return con


def get_repetition_count(con, sf, q):
    repetitions = con.execute(f"SELECT count(*) FROM {RESULTS_TABLE_NAME} WHERE sf = {sf} AND q = {q};").fetchall()[0][0]
    return REPETITIONS - repetitions


def insert_result(con, sf, q, t):
    con.execute(f"INSERT INTO {RESULTS_TABLE_NAME} VALUES ({sf}, {q}, {t});")


@timeout(600)
def timeout_fun(fun, query, *args):
    before = time.time()
    res = fun(query, *args)
    t = time.time() - before
    del res
    return t

def run_query(con, sf, q, query, fun, *args):
    repetitions = get_repetition_count(con, sf, q)
    error = 1
    for _ in range(repetitions):
        if error < 0:
            t = error
        else:
            try:
                t = timeout_fun(fun, query, *args)
            except TimeoutError:
                t = -1
            except Exception as e:
                raise e
                t = -2
            error = t
        insert_result(con, sf, q, t)


def run_benchmark(name, schema_fun, query_fun, *args):
    results_con = get_results_con(name)
    for sf in SCALE_FACTORS:
        counts_con = duckdb.connect(f'{BASE_DIR}/benchmark/duckdb/mydb.duckdb', read_only=True)
        counts_con.execute(f"USE sf{sf};")
        count = counts_con.execute("SELECT count(*) FROM lineitem;").fetchall()[0][0]
        counts_con.close()

        schema_fun(sf, *args)
        queries = get_queries()
        print(f'Running {name} SF{sf} ...')
        for q, query in tqdm.tqdm(queries):
            run_query(results_con, sf, q, query.replace('%OFFSET%', f'{count - 1}'), query_fun, *args)
        print(f'Running {name} SF{sf} done.')

    results_con.close()