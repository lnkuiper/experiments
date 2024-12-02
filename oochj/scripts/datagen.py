import os
import duckdb
import multiprocessing
import shutil
import sys


SCRIPTS_DIR = os.path.dirname(__file__)
sys.path.append(f'{SCRIPTS_DIR}/..')
from benchmark.util.util import *


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





def config_to_copy_statement(config):
    return f"""COPY (
    WITH cte AS (
        SELECT
            rand AS "0",
            1 - "0" AS "1",
            deterministic_random("0") AS "2",
            deterministic_random("1") AS "3",
            generalized_inverse(rand, {config['alpha']}, 1e-9, {config['key_count']}) / ({config['key_count']} + 1) AS "gi0",
            1 - "gi0" AS "gi1",
            deterministic_random("gi0") AS "gi2",
            deterministic_random("gi1") AS "gi3",
        FROM
            random
    )
    SELECT
        CAST(COLUMNS('^gi[0-9]') * {config['key_count']} AS BIGINT) AS "key_\0",
        random_to_bigint(COLUMNS('^[0-9]')) AS "int_\0",
        tag(COLUMNS('^[0-9]')) AS "tag_\0",
        employee(COLUMNS('^[0-9]')) AS "emp_\0",
        lorem_sentence(COLUMNS('^[0-9]'), 4) AS "com_\0",
    FROM
        cte
) TO '{config_to_string(config)}.csv' (FORMAT CSV);
"""



def main():



if __name__ == '__main__':
    main()
