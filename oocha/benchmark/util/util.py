import copy
import duckdb
import os
import time
import tqdm


BASE_DIR = f'{os.path.dirname(__file__)}/../..'

QUERIES_DIR = f'{BASE_DIR}/queries'
THIN_QUERIES_DIR = f'{QUERIES_DIR}/thin'
WIDE_QUERIES_DIR = f'{QUERIES_DIR}/wide'

DATA_DIR = f'{BASE_DIR}/data'

RESULTS_DIR = f'{BASE_DIR}/results'

REPETITIONS = 5
RESULTS_TABLE_NAME = 'results'
RESULTS_TABLE_COLS = [
    'grouping VARCHAR',
    'wide BOOLEAN',
    'runtime DOUBLE'
]

SCALE_FACTORS = [1] #, 10, 100]

SCHEMA_DIR = f'{BASE_DIR}/schema'


def get_schema():
    with open(f'{SCHEMA_DIR}/schema.sql', 'r') as f:
        return f.read()


def get_csv_path(sf):
    return f'{DATA_DIR}/lineitem_sf{sf}.csv'


def get_queries():
    queries = []
    for wide in [True, False]:
        source_dir = WIDE_QUERIES_DIR if wide else THIN_QUERIES_DIR
        for file_name in os.listdir(source_dir):
            file_path = f'{THIN_QUERIES_DIR}/{file_name}'
            with open(file_path, 'r') as f:
                queries.append((file_name.split('.')[0], wide, f.read()))
    return queries


def get_results_con(name):
    if not os.path.exists(RESULTS_DIR):
        os.mkdir(RESULTS_DIR)
    con = duckdb.connect(f'{RESULTS_DIR}/{name}.db')
    con.execute(f"""CREATE TABLE IF NOT EXISTS {RESULTS_TABLE_NAME} ({', '.join(RESULTS_TABLE_COLS)});""")
    return con


def get_repetition_count(con, grouping, wide):
    repetitions = con.execute(f"""SELECT count(*) FROM {RESULTS_TABLE_NAME} WHERE grouping = '{grouping}' AND wide = {wide};""").fetchall()[0][0]
    return REPETITIONS - repetitions


def insert_result(con, grouping, wide, runtime):
    con.execute(f"""INSERT INTO {RESULTS_TABLE_NAME} VALUES ('{grouping}', {wide}, {runtime});""")


def run_query(con, grouping, wide, query, fun, *args):
    repetitions = get_repetition_count(con, grouping, wide)
    for _ in range(repetitions):
        before = time.time()
        fun(query, *args)
        runtime = time.time() - before
        insert_result(con, grouping, wide, runtime)


def run_benchmark(name, fun, *args):
    con = get_results_con(name)
    queries = get_queries()
    for grouping, wide, query in tqdm.tqdm(queries):
        run_query(con, grouping, wide, query, fun, *args)
    con.close()
