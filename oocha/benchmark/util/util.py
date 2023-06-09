import copy
import duckdb
import os
import time
import tqdm


BASE_DIR = os.path.dirname(__file__) + '/../..'
DATA_DIR = f'{BASE_DIR}/data'
SOURCE_DATA_DIR = f'{DATA_DIR}/source'
GROUPS_DATA_DIR = f'{DATA_DIR}/groups'
RESULTS_DIR = f'{BASE_DIR}/results'

REPETITIONS = 5
RESULTS_TABLE_NAME = 'results'
RESULTS_TABLE_COLS = [
    'total_count UBIGINT',
    'type VARCHAR',
    'column_count USMALLINT',
    'power USMALLINT',
    'group_count UBIGINT',
    'threads USMALLINT',
    'time DOUBLE'
]


def load_config(con):
    return con.execute(f"SELECT config FROM '{BASE_DIR}/config.json' AS config").fetchall()[0][0]


def permute_dicts_internal(config, dicts, key):
    result = []
    for val in config[key]:
        for d in dicts:
            d_copy = copy.deepcopy(d)
            d_copy[key] = val
            result.append(d_copy)
    return result


def permute_dicts(config):
    configs = [{}]
    for key in config:
        configs = permute_dicts_internal(config, configs, key)
    return configs


def conf_to_str(conf):
    return str(conf).replace(' ','').replace("'", '`')


def get_repetition_count(con, config, threads):
    repetitions = con.execute(f"""
        SELECT count(*)
        FROM {RESULTS_TABLE_NAME}
        WHERE total_count = {config['total_count']}
          AND type = '{config['type']}'
          AND column_count = {config['column_count']}
          AND power = {config['power']}
          AND group_count = {config['group_count']}
          AND threads = {threads}
    """).fetchall()[0][0]
    return REPETITIONS - repetitions


def get_results_con(name):
    if not os.path.exists(RESULTS_DIR):
        os.mkdir(RESULTS_DIR)
    return duckdb.connect(f'{RESULTS_DIR}/{name}.db')
