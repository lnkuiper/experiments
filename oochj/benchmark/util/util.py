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

SCALE_FACTORS = [1, 2, 4, 8, 16, 32, 64, 128, 256]

REPETITIONS = 5
RESULTS_TABLE_NAME = 'results'
RESULTS_TABLE_COLS = [
    'sf USMALLINT',
    'q UTINYINT',
    'thin BOOLEAN',
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


def get_repetition_count(con, name, sf, q, thin):
    repetitions = con.execute(f"SELECT count(*) FROM {RESULTS_TABLE_NAME} WHERE sf = {sf} AND q = {q} AND thin = {thin};").fetchall()[0][0]
    return REPETITIONS - repetitions


def insert_result(con, name, sf, q, thin, t):
    con.execute(f"INSERT INTO {RESULTS_TABLE_NAME} VALUES ({sf}, {q}, {thin}, {t});")

@timeout(600)
def timeout_fun(fun, query, *args):
    before = time.time()
    res = fun(query, *args)
    t = time.time() - before
    del res
    return t

def run_query(name, result_con, sf, q, thin, query, fun, *args):
    if thin:
        query = f'SELECT count(*) FROM ({query});'
    else:
        query = f'{query};'
    repetitions = get_repetition_count(result_con, name, sf, q, thin)
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
                t = -2
            error = t
        insert_result(result_con, name, sf, q, thin, t)


def run_benchmark(name, schema_fun, query_fun, *args):
    duckdb.sql("SET threads=1;")
    result_con = get_results_con(name)
    result_con.execute("SET threads=1;")
    result_con.execute("SET memory_limit='50mb';")
    for sf in SCALE_FACTORS:
        counts = {
            1: 6001215,
            2: 11997996,
            4: 23996604,
            8: 47989007,
            16: 95988640,
            32: 192000551,
            64: 384016850,
            128: 768046938,
            256: 1536002440,
        }
        count = counts.get(sf)

        schema_fun(sf, *args)
        queries = get_queries()
        print(f'Running {name} SF{sf} ...')
        for q, query in tqdm.tqdm(queries):
            offset_query = query.replace('%OFFSET%', f'{count - 1}')
            run_query(name, result_con, sf, q, True, offset_query, query_fun, *args)
            if q != '6':
                run_query(name, result_con, sf, q, False, offset_query, query_fun, *args)
        print(f'Running {name} SF{sf} done.')
