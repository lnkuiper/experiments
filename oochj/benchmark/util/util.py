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
    repetitions = con.execute(f"SELECT count(*) FROM results WHERE experiment = '{experiment}' AND parameter = '{parameter}' AND value = '{value}';").fetchall()[0][0]
    return REPETITIONS - repetitions


def insert_result(con, experiment, parameter, value, t):
    con.execute(f"INSERT INTO results VALUES ('{experiment}', '{parameter}', '{value}', {t});")


################################################################################################################################
# CONFIG
################################################################################################################################
CONFIG_DIR = f'{BASE_DIR}/configs'

def get_config(config_name):
    return duckdb.execute(f"SELECT cfg FROM '{CONFIG_DIR}/{config_name}.json' AS cfg").fetchall()[0][0]


def supplied_or_default(default_config, supplied_parameter, supplied_value, parameter):
    if parameter == supplied_parameter:
        return supplied_value
    else:
        return default_config[parameter]


def get_build_table_config(parameter, value):
    default_config = get_config('default')

    row_count_build = supplied_or_default(default_config, parameter, value, 'row_count_build')
    key_count_multiplier = supplied_or_default(default_config, parameter, value, 'key_count_multiplier')
    return {
        'row_count': row_count_build,
        'alpha': supplied_or_default(default_config, parameter, value, 'alpha_build'),
        'key_count': key_count_multiplier * row_count_build,
    }


def get_probe_table_config(parameter, value):
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

TABLE_SCHEMA = """CREATE OR REPLACE TABLE "%TABLE_NAME%" (
    key_gi0 BIGINT,
    key_gi1 BIGINT,
    key_gi2 BIGINT,
    key_gi3 BIGINT,
    int_0 BIGINT,
    int_1 BIGINT,
    int_2 BIGINT,
    int_3 BIGINT,
    tag_0 VARCHAR,
    tag_1 VARCHAR,
    tag_2 VARCHAR,
    tag_3 VARCHAR,
    emp_0 VARCHAR,
    emp_1 VARCHAR,
    emp_2 VARCHAR,
    emp_3 VARCHAR,
    com_0 VARCHAR,
    com_1 VARCHAR,
    com_2 VARCHAR,
    com_3 VARCHAR
);
"""


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
    con.sql("CREATE OR REPLACE MACRO lorem_word(rand) AS ['voluptatem', 'quaerat', 'quiquia', 'non', 'dolore', 'dolorem', 'labore', 'consectetur', 'porro', 'sed', 'numquam', 'aliquam', 'sit', 'eius', 'modi', 'est', 'amet', 'magnam', 'dolor', 'etincidunt', 'velit', 'neque', 'ipsum', 'adipisci', 'quisquam', 'ut', 'tempora'][1 + floor(rand * 27 % 27)::BIGINT];")
    con.sql("CREATE OR REPLACE MACRO lorem_sentence_util(s) AS upper(s[1]) || s[2:] || '.';")
    con.sql("CREATE OR REPLACE MACRO lorem_sentence(rand, words) AS lorem_sentence_util(list_aggr([lorem_word(deterministic_random(rand + i)) for i in range(words)], 'string_agg', ' '));")


def table_config_to_filename(table_config):
    config_string = f"k{int(table_config['key_count'] / 1_000_000)}M_"
    config_string += f"a{table_config['alpha']}_"
    config_string += f"r{int(table_config['row_count'] / 1_000_000)}M"
    return f'{DATA_DIR}/{config_string}.csv'


def generate_data_from_table_config(table_config):
    filename = table_config_to_filename(table_config)
    if os.path.exists(filename):
        return
    print(f"Generating {filename.split("/")[-1]} ...", flush=True)
    con = duckdb.connect()
    initialize_datagen_macros(con)
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
        CAST(COLUMNS('^gi[0-9]') * {table_config['key_count']} AS BIGINT) AS "key_\\0",
        random_to_bigint(COLUMNS('^[0-9]')) AS "int_\\0",
        tag(COLUMNS('^[0-9]')) AS "tag_\\0",
        employee(COLUMNS('^[0-9]')) AS "emp_\\0",
        lorem_sentence(COLUMNS('^[0-9]'), 4) AS "com_\\0",
    FROM
        cte
) TO '{filename}' (FORMAT CSV);""")
    print(f"Generating {filename.split("/")[-1]} done.", flush=True)


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
def get_count(name, functions, experiment, parameter, value, query, *args):
    if not os.path.exists(RESULTS_DIR):
        os.mkdir(RESULTS_DIR)
    con = duckdb.connect(f'{RESULTS_DIR}/counts.duckdb')
    con.execute(f"CREATE TABLE IF NOT EXISTS counts (experiment VARCHAR, parameter VARCHAR, value VARCHAR, c UBIGINT);")

    res = con.execute(f"SELECT c FROM counts WHERE experiment = '{experiment}' AND parameter = '{parameter}' AND value = '{value}';").fetchall()
    if res:
        return res[0][0]

    assert(name == 'duckdb')
    query = query.replace('%OFFSET%', '0')
    c = functions['query'](f"SELECT count(*) FROM ({query});", *args)[0][0]
    con.execute(f"INSERT INTO counts VALUES ('{experiment}', '{parameter}', '{value}', {c});")

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


def get_query(experiment, parameter, value):
    default_config = get_config('default')
    if experiment == 'join':
        default_config['parameter'] = value
        return f"""SELECT 
    {',\n    '.join(['b.int_' + str(i) for i in range(default_config['payload_columns_build'])])},
    {',\n    '.join(['b.tag_' + str(i) for i in range(default_config['payload_columns_build'])])},
    {',\n    '.join(['b.emp_' + str(i) for i in range(default_config['payload_columns_build'])])},
    {',\n    '.join(['b.com_' + str(i) for i in range(default_config['payload_columns_build'])])},
    {',\n    '.join(['p.int_' + str(i) for i in range(default_config['payload_columns_build'])])},
    {',\n    '.join(['p.tag_' + str(i) for i in range(default_config['payload_columns_build'])])},
    {',\n    '.join(['p.emp_' + str(i) for i in range(default_config['payload_columns_build'])])},
    {',\n    '.join(['p.com_' + str(i) for i in range(default_config['payload_columns_build'])])}
FROM
    build b,
    probe p
WHERE
    {'\n AND '.join(['b.key_gi' + str(i) + ' = p.key_gi' + str(i) for i in range(default_config['key_columns'])])}
OFFSET
    %OFFSET%
        """
    else:
        # TODO
        assert(False)


def run_config(name, functions, results_con, experiment, parameter, value, repetitions, *args):
    query = get_query(experiment, parameter, value)
    count = get_count(name, functions, experiment, parameter, value, query, *args)
    query = query.replace('%OFFSET%', f'{count - 1}')

    print("Querying ...", flush=True)
    error = 1
    for _ in tqdm.tqdm(range(repetitions)):
        t = 0
        res = None
        if error < 0:
            t = error
        else:
            try:
                t, res = timeout_fun(functions['query'], query, *args)
            except TimeoutError:
                t = -1
            except Exception as e:
                raise e
                t = -2
            finally:
                functions['close'](res, *args)
            error = t
        insert_result(results_con, experiment, parameter, value, t)
    print("Querying done.", flush=True)


def wrap_load(functions, table_alias, table_schema, filename, *args):
    filename_short = filename.split('/')[-1].replace('.csv', '')
    if not functions['already_loaded'](filename_short, *args):
        print(f"Loading {filename_short} ...", flush=True)
        functions['load'](table_schema, filename, filename_short, *args)
        print(f"Loading {filename_short} done.", flush=True)
    functions['create_view'](table_alias, filename_short, *args)


def run_experiments(name, functions, *args):
    results_con = get_results_con(name)
    for experiment in EXPERIMENTS:
        experiment_config = get_config(experiment)
        for parameter in experiment_config:
            for value in experiment_config[parameter][:1]: # TODO
                repetitions = get_repetition_count(results_con, experiment, parameter, value)
                if repetitions == 0:
                    continue
                print(f'Running {name} {experiment} {parameter} {value} ...', flush=True)

                build_table_config = get_build_table_config(parameter, value)
                generate_data_from_table_config(build_table_config)
                wrap_load(functions, 'build', TABLE_SCHEMA, table_config_to_filename(build_table_config), *args)

                probe_table_config = get_probe_table_config(parameter, value)
                generate_data_from_table_config(probe_table_config)
                wrap_load(functions, 'probe', TABLE_SCHEMA, table_config_to_filename(probe_table_config), *args)

                run_config(name, functions, results_con, experiment, parameter, value, repetitions, *args)

                print(f'Running {name} {experiment} {parameter} {value} done.', flush=True)
