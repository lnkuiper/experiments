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


################################################################################################################################
# DATA
################################################################################################################################
DATA_DIR = f'{BASE_DIR}/data'

TABLE_SCHEMA = """CREATE TABLE IF NOT EXISTS %TABLE_NAME% (
    "key_c100M_a0.0" BIGINT,
    "key_c120M_a0.0" BIGINT,
    "key_c160M_a0.0" BIGINT,
    "key_c200M_a0.0" BIGINT,
    "key_c300M_a0.0" BIGINT,
    "key_c40M_a0.0" BIGINT,
    "key_c400M_a0.0" BIGINT,
    "key_c500M_a0.0" BIGINT,
    "key_c80M_a0.0" BIGINT,
    "key_c100M_a0.25" BIGINT,
    "key_c200M_a0.25" BIGINT,
    "key_c100M_a0.5" BIGINT,
    "key_c120M_a0.5" BIGINT,
    "key_c160M_a0.5" BIGINT,
    "key_c200M_a0.5" BIGINT,
    "key_c300M_a0.5" BIGINT,
    "key_c40M_a0.5" BIGINT,
    "key_c400M_a0.5" BIGINT,
    "key_c500M_a0.5" BIGINT,
    "key_c80M_a0.5" BIGINT,
    "key_c100M_a0.75" BIGINT,
    "key_c200M_a0.75" BIGINT,
    "key_c100M_a1.0" BIGINT,
    "key_c200M_a1.0" BIGINT,
    "key_c200M_a0.0_1" BIGINT,
    "key_c200M_a0.5_1" BIGINT,
    "tag_0" VARCHAR,
    "tag_1" VARCHAR,
    "tag_2" VARCHAR,
    "tag_3" VARCHAR,
    "emp_0" VARCHAR,
    "emp_1" VARCHAR,
    "emp_2" VARCHAR,
    "emp_3" VARCHAR,
    "com_0" VARCHAR,
    "com_1" VARCHAR,
    "com_2" VARCHAR,
    "com_3" VARCHAR
);
"""


def row_count_to_table_name(row_count):
    return f"r{int(row_count / 1_000_000)}M"


def row_count_to_file_list(row_count):
    file_count = int(row_count / 10_000_000)
    return [f'{DATA_DIR}/split{i + 1}.csv' for i in range(file_count)]


def col_name(key_count, alpha):
    return f'key_c{int(key_count / 1_000_000)}M_a{alpha}'


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

    #assert(name == 'duckdb')
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

    build_config = default_config['build']
    probe_row_count = default_config['probe']['row_count']
    build_payload_columns = build_config['payload_columns']
    probe_payload_columns = default_config['probe']['payload_columns']

    if experiment == 'join':
        build_row_count = build_config['row_count']
        if parameter == 'build_row_count':
            build_row_count = value
        if parameter == 'probe_row_count':
            probe_row_count = value

        if parameter == 'build_payload_columns':
            build_payload_columns = value
        if parameter == 'probe_payload_columns':
            probe_payload_columns = value

        key_count = build_row_count
        if parameter == 'key_count':
            key_count = value

        build_alpha = build_config['alpha']
        probe_alpha = default_config['probe']['alpha']
        if parameter == 'build_alpha':
            build_alpha = value
            probe_alpha = 0.0
        if parameter == 'probe_alpha':
            probe_alpha = value
            build_alpha = 0.0

        build_table_name = row_count_to_table_name(build_row_count)
        probe_table_name = row_count_to_table_name(probe_row_count)

        build_col_name = col_name(key_count, build_alpha)
        probe_col_name = col_name(key_count, probe_alpha)
        if build_col_name == probe_col_name:
            build_col_name += "_1"

        default_config['parameter'] = value
        return f"""SELECT 
    {',\n    '.join([c for c in [
        ',\n    '.join(['any_value(b.tag_' + str(i) + ')' for i in range(build_payload_columns)]),
        ',\n    '.join(['any_value(b.emp_' + str(i) + ')' for i in range(build_payload_columns)]),
        ',\n    '.join(['any_value(b.com_' + str(i) + ')' for i in range(build_payload_columns)]),
        ',\n    '.join(['any_value(p.tag_' + str(i) + ')' for i in range(probe_payload_columns)]),
        ',\n    '.join(['any_value(p.emp_' + str(i) + ')' for i in range(probe_payload_columns)]),
        ',\n    '.join(['any_value(p.com_' + str(i) + ')' for i in range(probe_payload_columns)]),
    ] if c])}
FROM
    {probe_table_name} p
JOIN
    {build_table_name} b
ON
    (b."{build_col_name}" = p."{probe_col_name}")
        """
#OFFSET
#    %OFFSET%
    elif experiment == 'pipeline':
        build_tables = [row_count_to_table_name(brc) for brc in value]
        key_column = col_name(probe_row_count, 0.0)

        columns = []
        columns += ['p.tag_' + str(i) for i in range(probe_payload_columns)]
        columns += ['p.emp_' + str(i) for i in range(probe_payload_columns)]
        columns += ['p.com_' + str(i) for i in range(probe_payload_columns)]

        build_aliases = []
        conditions = []
        for i, build_table in enumerate(build_tables):
            build_alias = f'b{i + 1}'
            build_aliases.append(build_alias)
            conditions.append(f'ON\n        p."{key_column}" = {build_alias}."{key_column}"')

            columns += [f'{build_alias}.tag_' + str(i) for i in range(build_payload_columns)]
            columns += [f'{build_alias}.emp_' + str(i) for i in range(build_payload_columns)]
            columns += [f'{build_alias}.com_' + str(i) for i in range(build_payload_columns)]

        return f"""SELECT
    {',\n    '.join(columns)}
FROM {row_count_to_table_name(probe_row_count)} p
LEFT JOIN
{'\nLEFT JOIN\n'.join(['        ' + bt + ' AS ' + ba + '\n' + c for bt, ba, c in zip(build_tables, build_aliases, conditions)])}
OFFSET
    %OFFSET%
        """
        # TODO
        assert(False)
    else:
        assert(False)


def run_config(name, functions, results_con, experiment, parameter, value, repetitions, *args):
    query = get_query(experiment, parameter, value)
    count = get_count(name, functions, experiment, parameter, value, query, *args)
    query = query.replace('%OFFSET%', f'{count - 1}')

    print("Querying ...")
    error = 1
    for _ in tqdm.tqdm(range(repetitions)):
        t = 0
        res = None
        e = None
        if error < 0:
            t = error
        else:
            try:
                t, res = timeout_fun(functions['query'], query, *args)
            except TimeoutError as ex:
                t = -1
                e = str(ex)
            except Exception as ex:
                t = -2
                e = str(ex)
            finally:
                if e:
                    print(e)
                    e = None
                if res:
                    functions['close'](res, *args)
            error = t
        insert_result(results_con, experiment, parameter, value, t)
    print("Querying done.")


def wrap_load(name, functions, row_count, *args):
    table_name = row_count_to_table_name(row_count)
    if not functions['already_loaded'](row_count, *args):
        print(f"Loading {name} {table_name} ...")
        functions['load'](row_count, *args)
        print(f"Loading {name} {table_name} done.")
    if row_count == 100_000_000 or row_count == 200_000_000 or row_count == 500_000_000:
        return None
    return row_count


def run_experiments(name, functions, *args):
    default_config = get_config('default')

    results_con = get_results_con(name)
    for experiment in EXPERIMENTS:
        wrap_load(name, functions, default_config['probe']['row_count'], *args)
        policy_experiment = name in ['weightedcost', 'unweightedcost', 'equality', 'equity']
        if experiment == 'join':
            if policy_experiment:
                continue
            wrap_load(name, functions, default_config['build']['row_count'], *args)
        elif not policy_experiment:
            continue

        experiment_config = get_config(experiment)
        for parameter in experiment_config:
            for value in experiment_config[parameter]:
                repetitions = get_repetition_count(results_con, experiment, parameter, value)
                if repetitions == 0:
                    continue
                print(f'Running {name} {experiment} {parameter} {value} ...')

                loaded_table_row_counts = []
                if parameter == 'scenario':
                    for row_count in set(value):
                        loaded_table_row_counts.append(wrap_load(name, functions, row_count, *args))
                elif parameter == 'build_row_count' or parameter == 'probe_row_count':
                    loaded_table_row_counts.append(wrap_load(name, functions, value, *args))
                run_config(name, functions, results_con, experiment, parameter, value, repetitions, *args)
                for row_count in loaded_table_row_counts:
                    if row_count:
                        functions['drop'](row_count, *args)

                print(f'Running {name} {experiment} {parameter} {value} done.')
