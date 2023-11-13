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

# up to sf128
SCALE_FACTORS = [1 << i for i in range(2)]

SCHEMA_DIR = f'{BASE_DIR}/schema'

COUNTS = {'l_orderkey-l_suppkey': {1: 5999989, 2: 11996779}, 'l_suppkey-l_partkey-l_shipmode': {1: 3684267, 2: 7372577}, 'l_returnflag-l_linestatus': {1: 4, 2: 4}, 'l_orderkey': {1: 1500000, 2: 3000000}, 'l_suppkey-l_returnflag-l_linestatus': {1: 39806, 2: 79568}, 'l_suppkey-l_partkey-l_shipinstruct': {1: 2710396, 2: 5423676}, 'l_suppkey-l_partkey-l_returnflag-l_linestatus': {1: 2167877, 2: 4336381}, 'l_suppkey': {1: 10000, 2: 20000}, 'l_partkey': {1: 200000, 2: 400000}, 'l_suppkey-l_partkey-l_orderkey': {1: 6001204, 2: 11997981}, 'l_partkey-l_returnflag-l_linestatus': {1: 634993, 2: 1269566}, 'l_suppkey-l_partkey': {1: 799541, 2: 1599085}, 'l_shipmode': {1: 7, 2: 7}, 'l_suppkey-l_partkey-l_shipinstruct-l_shipmode': {1: 5270225, 2: 10548748}, 'l_orderkey-l_returnflag-l_linestatus': {1: 2091229, 2: 4182363}, 'l_orderkey-l_partkey': {1: 6001169, 2: 11997938}}


def get_schema(sf, clickhouse=False):
    if clickhouse:
        with open(f'{SCHEMA_DIR}/clickhouse_schema.sql', 'r') as f:
            schema = f.read()
    else:
        with open(f'{SCHEMA_DIR}/schema.sql', 'r') as f:
            schema = f.read()
    return schema.replace('lineitem', f'lineitem{sf}')


def get_csv_path(sf):
    return f'{DATA_DIR}/lineitem_sf{sf}.csv'


def get_queries():
    queries = []
    for wide in [False, True]:
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
    for sf in SCALE_FACTORS:
        print(f'Running {name} SF{sf} ...')
        con = get_results_con(name)
        queries = get_queries()
        for grouping, wide, query in tqdm.tqdm(queries):
            run_query(con, sf, grouping, wide, query.replace('lineitem', f'lineitem{sf}').replace('offset', f'{COUNTS[grouping][sf] - 1}'), fun, *args)
        con.close()
        print(f'Running {name} SF{sf} done.')
