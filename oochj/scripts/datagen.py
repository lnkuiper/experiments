import os
import copy
import duckdb
import multiprocessing
import numpy as np
import shutil
import sys


SCRIPTS_DIR = os.path.dirname(__file__)
sys.path.append(f'{SCRIPTS_DIR}/..')
from benchmark.util.util import *


SPLITS = 100


def initialize_datagen_macros(con):
    # Macro to generate a deterministic value between 0 and 1 from another value
    con.sql("CREATE OR REPLACE MACRO deterministic_random(rand) AS hash(rand) / 18446744073709551615;")
    # Macro for generalized inverse for generating skewed distributions (higher alpha = more skew)
    # When alpha = 0 it's random uniform, when alpha = 1 it's Zipfian
    con.sql("""CREATE OR REPLACE MACRO generalized_inverse(rand, rowid, alpha, xmin, xmax) AS
        CASE alpha
            WHEN 0 THEN
                rowid % xmax + 1
            WHEN 1 THEN 
                ceil(xmin * exp(rand * ln(xmax / xmin)))::BIGINT
            ELSE 
                ceil(((xmin^(1 - alpha)) + rand * ((xmax^(1 - alpha)) - (xmin^(1 - alpha))))^(1 / (1 - alpha)))::BIGINT
        END;""")
    # Macro to generate some kind of tag column
    con.sql("CREATE OR REPLACE MACRO tag(rand) AS 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'[1 + floor(rand * 26 % 26)::BIGINT];")
    # Macro to generate some kind of employee column
    con.sql("CREATE OR REPLACE MACRO employee(rand) AS printf('EMPNO%010d', CAST(rand * 1_000_000_000 AS BIGINT));")
    # Some macros to generate lorem ipsum
    con.sql("CREATE OR REPLACE MACRO lorem_word(rand) AS ['voluptatem', 'quaerat', 'quiquia', 'non', 'dolore', 'dolorem', 'labore', 'consectetur', 'porro', 'sed', 'numquam', 'aliquam', 'sit', 'eius', 'modi', 'est', 'amet', 'magnam', 'dolor', 'etincidunt', 'velit', 'neque', 'ipsum', 'adipisci', 'quisquam', 'ut', 'tempora'][1 + floor(rand * 27 % 27)::BIGINT];")
    con.sql("CREATE OR REPLACE MACRO lorem_sentence_util(s) AS upper(s[1]) || s[2:] || '.';")
    con.sql("CREATE OR REPLACE MACRO lorem_sentence(rand, words) AS lorem_sentence_util(list_aggr([lorem_word(deterministic_random(rand + i)) for i in range(words)], 'string_agg', ' '));")


def generate_split(split):
    filename = f"{DATA_DIR}/split{split}.csv"
    if os.path.exists(filename):
        return

    # Initialize
    split_size = int(1_000_000_000 / SPLITS)
    rowid_offset = (split - 1) * split_size
    con = duckdb.connect()
    con.execute("SET threads=1;")
    con.execute("PRAGMA disable_progress_bar;")
    initialize_datagen_macros(con)
    con.execute(f"SELECT setseed({(split / SPLITS)**2});")
    con.execute(f"CREATE TABLE random AS SELECT random() AS rand FROM range({split_size});")

    # Generate key columns
    alphas = list(np.linspace(0, 1, 5)) # 0.25 increments
    key_counts = [int(x) for x in np.linspace(40_000_000, 200_000_000, 5)] # 40M increments
    key_counts += [i * 100_000_000 for i in range(3, 6)] # 100M increments

    # Generate all kinds of different keys at once, changing only one variable at a time
    keys = []
    # We have varying alpha with fixed key counts at 100M/200M
    for key_count in [100_000_000, 200_000_000]:
        keys+= [{'key_count': key_count, 'alpha': alpha} for alpha in alphas]
    # We have varying key counts with fixed alphas at 0.0/0.5
    for alpha in [0.0, 0.5]:
        keys += [{'key_count': key_count, 'alpha': alpha} for key_count in key_counts]

    # Generate column definitions
    key_columns = [f"""generalized_inverse(rand, {rowid_offset} + rowid, {key['alpha']}, 1e-42, {key['key_count']}) AS "{col_name(key['key_count'], key['alpha'])}\"""" for key in keys]
    key_columns = sorted(list(set(key_columns)))
    # We need to "duplicate" these columns otherwise we risk joining the EXACT same columns - messes up statistical properties
    key_columns += [
        f'generalized_inverse(deterministic_random(rand), {rowid_offset} + rowid, 0.0, 1e-42, 200_000_000) AS "{col_name(200_000_000, 0.0)}_1"',
        f'generalized_inverse(deterministic_random(rand), {rowid_offset} + rowid, 0.5, 1e-42, 200_000_000) AS "{col_name(200_000_000, 0.5)}_1"',
    ]

    # Generate the data
    q = f"""COPY (
    WITH cte AS (
        SELECT
            rand AS "0",
            1 - "0" AS "1",
            deterministic_random("0") AS "2",
            deterministic_random("1") AS "3",
            {',\n            '.join(key_columns)},
        FROM
            random
    )
    SELECT
        COLUMNS('^key'),
        tag(COLUMNS('^[0-9]')) AS "tag_\\0",
        employee(COLUMNS('^[0-9]')) AS "emp_\\0",
        lorem_sentence(COLUMNS('^[0-9]'), 4) AS "com_\\0",
    FROM
        cte
) TO '{filename}';"""
    con.execute(q)


def main():
    if not os.path.exists(DATA_DIR):
        os.mkdir(DATA_DIR)
    print("Generating data ...")
    with multiprocessing.Pool(multiprocessing.cpu_count() * 2) as p:
        p.map(generate_split, range(1, SPLITS + 1))
    print("Generating data done.")


if __name__ == '__main__':
    main()
