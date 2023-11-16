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
    'sf USMALLINT',
    'grouping VARCHAR',
    'wide BOOLEAN',
    'runtime DOUBLE'
]

# 8 for up to sf128
SCALE_FACTORS = [1 << i for i in range(8)]

SCHEMA_DIR = f'{BASE_DIR}/schema'


def get_schema(sf, system=None):
    if system:
        with open(f'{SCHEMA_DIR}/{system}_schema.sql', 'r') as f:
            schema = f.read()
    else:
        with open(f'{SCHEMA_DIR}/schema.sql', 'r') as f:
            schema = f.read()
    return schema.replace('lineitem', f'lineitem{sf}')


def get_csv_path(sf):
    return f'{DATA_DIR}/lineitem_sf{sf}.csv'


def get_queries(thin_only=False):
    wides = [False] if thin_only else [False, True]
    queries = []
    for wide in wides:
        source_dir = WIDE_QUERIES_DIR if wide else THIN_QUERIES_DIR
        for file_name in os.listdir(source_dir):
            file_path = f'{source_dir}/{file_name}'
            with open(file_path, 'r') as f:
                queries.append((file_name.split('.')[0], wide, f.read()))
    return queries


def get_results_con(name):
    if not os.path.exists(RESULTS_DIR):
        os.mkdir(RESULTS_DIR)
    con = duckdb.connect(f'{RESULTS_DIR}/{name}.db')
    con.execute(f"""CREATE TABLE IF NOT EXISTS {RESULTS_TABLE_NAME} ({', '.join(RESULTS_TABLE_COLS)});""")
    return con


def get_repetition_count(con, sf, grouping, wide):
    repetitions = con.execute(f"""SELECT count(*) FROM {RESULTS_TABLE_NAME} WHERE sf = {sf} AND grouping = '{grouping}' AND wide = {wide};""").fetchall()[0][0]
    return REPETITIONS - repetitions


def insert_result(con, sf, grouping, wide, runtime):
    con.execute(f"""INSERT INTO {RESULTS_TABLE_NAME} VALUES ({sf}, '{grouping}', {wide}, {runtime});""")


def run_query(con, sf, grouping, wide, query, fun, *args):
    repetitions = get_repetition_count(con, sf, grouping, wide)
    for _ in range(repetitions):
        before = time.time()
        res = fun(query, *args)
        runtime = time.time() - before
        del res
        insert_result(con, sf, grouping, wide, runtime)


def run_benchmark(name, fun, *args):
    results_con = get_results_con(name)
    counts_con = duckdb.connect(f'{QUERIES_DIR}/counts.db')
    for sf in SCALE_FACTORS:
        print(f'Running {name} SF{sf} ...')
        queries = get_queries()
        for grouping, wide, query in tqdm.tqdm(queries):
            count = counts_con.execute(f"""SELECT c FROM counts WHERE grouping = '{grouping}' AND sf = {sf};""").fetchall()[0][0]
            run_query(results_con, sf, grouping, wide, query.replace('lineitem', f'lineitem{sf}').replace('offset', f'{count - 1}'), fun, *args)
        print(f'Running {name} SF{sf} done.')
    results_con.close()
