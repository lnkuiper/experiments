import copy
import duckdb
import os
import time
import tqdm
from timeout_decorator import timeout, TimeoutError

BASE_DIR = f'{os.path.dirname(__file__)}/../..'

################################################################################################################################
# RESULTS
################################################################################################################################
RESULTS_DIR = f'{BASE_DIR}/results'

REPETITIONS = 5
RESULTS_TABLE_COLS = [
    'experiment VARCHAR',
    'parameter VARCHAR'
    'value VARCHAR',
    't DOUBLE',
]

def get_results_con(name):
    if not os.path.exists(RESULTS_DIR):
        os.mkdir(RESULTS_DIR)
    con = duckdb.connect(f'{RESULTS_DIR}/{name}.duckdb')
    con.execute(f"CREATE TABLE IF NOT EXISTS results (experiment VARCHAR, parameter VARCHAR, value VARCHAR, t DOUBLE);")
    return con


def get_repetition_count(con, experiment, parameter, value):
    repetitions = con.execute(f"SELECT count(*) FROM results WHERE experiment = {experiment} AND parameter = {parameter} AND value = {value};").fetchall()[0][0]
    return REPETITIONS - repetitions


def insert_result(con, experiment, parameter, value, t):
    con.execute(f"INSERT INTO {RESULTS_TABLE_NAME} VALUES ({experiment}, {parameter}, {value}, {t});")


################################################################################################################################
# CONFIG
################################################################################################################################
CONFIG_DIR = f'{BASE_DIR}/configs'

def get_config(config_name):
    duckdb.execute(f"FROM '{CONFIG_DIR}/{config_name}.json'").fetchall()[0][0]


def supplied_or_default(default_config, supplied_parameter, supplied_value, parameter):
    if parameter == supplied_parameter:
        return supplied_value
    else:
        return default_config['parameter']


def get_build_table_config(parameter, value)
    default_config = get_config('default')

    row_count_build = supplied_or_default(default_config, parameter, value, 'row_count_build')
    key_count_multiplier = supplied_or_default(default_config, parameter, value, 'key_count_multiplier')
    return {
        'row_count': row_count_build,
        'alpha': supplied_or_default(default_config, parameter, value, 'alpha_build'),
        'key_count': key_count_multiplier * row_count_build,
    }


def get_probe_table_config(parameter, value)
    default_config = get_config('default')

    row_count_build = supplied_or_default(default_config, parameter, value, 'row_count_build')
    key_count_multiplier = supplied_or_default(default_config, parameter, value, 'key_count_multiplier')
    return {
        'row_count': supplied_or_default(default_config, parameter, value, 'row_count_probe'),
        'alpha': supplied_or_default(default_config, parameter, value, 'alpha_probe'),
        'key_count': key_count_multiplier * row_count_build,
    }


################################################################################################################################
# DATA
################################################################################################################################
DATA_DIR = f'{BASE_DIR}/data'

BUILD_SCHEMA = ''
PROBE_SCHEMA = ''

def initialize_datagen_macros(con):
    # Macro to generate a deterministic value between 0 and 1 from another value
    con.sql("CREATE OR REPLACE MACRO deterministic_random(rand) AS hash(rand) / 18446744073709551615;")
    # Macro for generalized inverse for generating skewed distributions (higher alpha = more skew)
    # When alpha = 0 it's random uniform, when alpha = 1 it's Zipfian
    con.sql("""CREATE OR REPLACE MACRO generalized_inverse(rand, alpha, xmin, xmax) AS
        CASE alpha
            WHEN 1 THEN 
                ceil(xmin * exp(rand * ln(xmax / xmin)))
            ELSE 
                ceil(((xmin^(1 - alpha)) + rand * ((xmax^(1 - alpha)) - (xmin^(1 - alpha))))^(1 / (1 - alpha)))
        END;""")
    # Macro to generate random BIGINT column between -1B and +1B
    con.sql("CREATE OR REPLACE MACRO random_to_bigint(rand) AS CAST(-1_000_000_000 + rand * 1_000_000_000 AS BIGINT);")
    # Macro to generate some kind of tag column
    con.sql("CREATE OR REPLACE MACRO tag(rand) AS 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'[1 + floor(rand * 26 % 26)::BIGINT];")
    # Macro to generate some kind of employee column
    con.sql("CREATE OR REPLACE MACRO employee(rand) AS printf('EMPNO%010d', CAST(rand * 1_000_000_000 AS BIGINT));")
    # Some macros to generate lorem ipsum
    con.sql("CREATE OR REPLACE MACRO lorem_word(rand) AS ['voluptatem', 'quaerat', 'quiquia', 'non', 'dolore', 'dolorem', 'labore', 'consectetur', 'porro', 'sed', 'numquam', 'aliquam', 'sit', 'eius', ")'modi', 'est', 'amet', 'magnam', 'dolor', 'etincidunt', 'velit', 'neque', 'ipsum', 'adipisci', 'quisquam', 'ut', 'tempora'][1 + floor(rand * 27 % 27)::BIGINT];
    con.sql("CREATE OR REPLACE MACRO lorem_sentence_util(s) AS upper(s[1]) || s[2:] || '.';")
    con.sql("CREATE OR REPLACE MACRO lorem_sentence(rand, words) AS lorem_sentence_util(list_aggr([lorem_word(deterministic_random(rand + i)) for i in range(words)], 'string_agg', ' '));")


def table_config_to_filename(table_config):
    config_string = f"k{config['key_count']}_"
    config_string += f"a{config['alpha']}_"
    config_string += f"r{int(config['row_count'] / 1_000_000)M}"
    return f'{DATA_DIR}/{config_string}.csv'


def generate_data_from_table_config(table_config):
    filename = table_config_to_filename(table_config)
    if os.path.exists(filename):
        return
    con = duckdb.connect()
    con.execute("SELECT setseed(0.42);")
    con.execute(f"CREATE TABLE random AS SELECT random() AS rand FROM range({table_config['row_count']});")
    con.execute("SET preserve_insertion_order=false;")
    con.execute(f"""COPY (
    WITH cte AS (
        SELECT
            rand AS "0",
            1 - "0" AS "1",
            deterministic_random("0") AS "2",
            deterministic_random("1") AS "3",
            generalized_inverse(rand, {table_config['alpha']}, 1e-9, {table_config['key_count']}) / ({table_config['key_count']} + 1) AS "gi0",
            1 - "gi0" AS "gi1",
            deterministic_random("gi0") AS "gi2",
            deterministic_random("gi1") AS "gi3",
        FROM
            random
    )
    SELECT
        CAST(COLUMNS('^gi[0-9]') * {table_config['key_count']} AS BIGINT) AS "key_\0",
        random_to_bigint(COLUMNS('^[0-9]')) AS "int_\0",
        tag(COLUMNS('^[0-9]')) AS "tag_\0",
        employee(COLUMNS('^[0-9]')) AS "emp_\0",
        lorem_sentence(COLUMNS('^[0-9]'), 4) AS "com_\0",
    FROM
        cte
) TO '{filename}' (FORMAT CSV);""")


def parameter_requires_regen(parameter):
    if parameter == 'row_count_build':
        return True
    elif parameter == 'alpha_build':
        return True
    elif parameter == 'key_count_multiplier':
        return True
    elif parameter == 'alpha_probe':
        return True
    else:
        return False


################################################################################################################################
# COUNTS
################################################################################################################################
def get_count(name, experiment, parameter, value, query, query_fun, *args):
    if not os.path.exists(RESULTS_DIR):
        os.mkdir(RESULTS_DIR)
    con = duckdb.connect(f'{RESULTS_DIR}/counts.duckdb')
    con.execute(f"CREATE TABLE IF NOT EXISTS counts (experiment VARCHAR, parameter VARCHAR, value VARCHAR, c UBIGINT);")

    res = con.execute(f"SELECT c FROM counts WHERE experiment = {experiment} AND parameter = {parameter} AND value = {value};").fetchall()
    if res:
        return res[0][0]

    assert(name == 'duckdb')
    c = query_fun(f"SELECT count(*) FROM ({query});", *args)[0][0]
    con.execute(f"INSERT INTO counts VALUES ({experiment}, {parameter}, {value}, {c});")

    return c


################################################################################################################################
# BENCHMARK
################################################################################################################################
EXPERIMENTS = [
    'join',
    'pipeline'
]

@timeout(1000)
def timeout_fun(fun, query, *args):
    before = time.time()
    res = fun(query, *args)
    t = time.time() - before
    return t, res


def run_config(name, functions, results_con, experiment, parameter, value, repetitions, *args):
    query = get_query(experiment, parameter, value)
    count = get_count(name, experiment, parameter, value, query, query_fun, *args)
    query.replace('%OFFSET%', f'{count - 1}')

    error = 1
    for _ in range(repetitions):
        t = 0
        res = None
        if error < 0:
            t = error
        else:
            try:
                t, res = timeout_fun(query_fun, functions['query'], *args)
            except TimeoutError:
                t = -1
            except Exception as e:
                raise e
                t = -2
            finally:
                functions['close'](res)
            error = t
        insert_result(results_con, experiment, parameter, value, t)


def run_experiments(name, functions, *args):
    results_con = get_results_con(name)
    for experiment in EXPERIMENTS:
        experiment_config = get_config(experiment)
        for parameter in experiment_config:
            print(f'Running {name} {experiment} {parameter} ...')
            for value in tqdm.tqdm(experiment_config[parameter]):
                repetitions = get_repetition_count(result_con, experiment, parameter, value)
                if repetitions == 0:
                    continue

                build_table_config = get_build_table_config(parameter, value)
                generate_data_from_table_config(build_table_config)
                functions['load']('build', BUILD_SCHEMA, table_config_to_filename(build_table_config))

                probe_table_config = get_probe_table_config(parameter, value)
                generate_data_from_table_config(probe_table_config)
                functions['load']('probe', PROBE_SCHEMA, table_config_to_filename(probe_table_config))

                run_config(name, functions, results_con, experiment, parameter, value, repetitions, *args)
            print(f'Running {name} {experiment} {parameter} done.')

