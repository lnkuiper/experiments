import lorem
import math
import numpy as np
import os
import pandas as pd
import random
import sys
import tqdm


SCRIPTS_DIR = os.path.dirname(__file__)
sys.path.append(f'{SCRIPTS_DIR}/..')
from benchmark.util.util import *


def create_distinct_string_data(con, name, min_strlen, max_strlen, count):
    file = f"{SOURCE_DATA_DIR}/{name}_strings.parquet"
    if os.path.exists(file):
        return
    
    l = [c for c in list(lorem.text()) if c != '\r' ]
    source_string = ''
    
    offset_in_source = 0
    strlen_range = max_strlen - min_strlen
    
    unique_strings = set()
    while (len(unique_strings) < count):
        strlen = min_strlen + math.ceil(random.random() * strlen_range)
        
        if offset_in_source + strlen > len(source_string):
            random.shuffle(l)
            source_string = ''.join(l)
            offset_in_source = 0
        
        unique_strings.add(source_string[offset_in_source:offset_in_source + strlen])
        offset_in_source += strlen
    
    unique_strings = pd.DataFrame(list(unique_strings), columns=['c0'])
    con.execute(f"""
    COPY (SELECT * FROM unique_strings)
    TO '{file}'
    """)


def create_distinct_integral_data(con, t, count):
    file = f"{SOURCE_DATA_DIR}/{t}s.parquet"
    if os.path.exists(file):
        return

    limits = {
        'tinyint': (1 << 7) - 1,
        'smallint': (1 << 15) - 1,
        'integer': (1 << 31) - 1,
        'bigint': (1 << 63) - 1,
    }
    
    limit = limits[t]
    con.execute(f"""
    COPY (SELECT DISTINCT CAST(random() * {limit} - {limit} / 2 AS {t}) c0
          FROM range({count})
    ) TO '{file}'
    """)


def generate_source_data():
    if not os.path.exists(SOURCE_DATA_DIR):
        os.mkdir(SOURCE_DATA_DIR)

    con = duckdb.connect()

    string_params = [
        ('tiny', 1, 1, 20),
        ('small', 4, 7, int(1e6)),
        ('medium', 8, 15, int(1e6)),
        ('large', 16, 31, int(1e6)),
        ('huge', 32, 63, int(1e6)),
    ]

    print('Generating string data ...')
    for param in tqdm.tqdm(string_params):
        create_distinct_string_data(con, *param)
    print('Done.')

    integral_params = [
        ('tinyint', 100),
        ('smallint', 10000),
        ('integer', int(1e8)),
        ('bigint', int(1e8)),
    ]

    print('Generating integral data ...')
    for param in tqdm.tqdm(integral_params):
        create_distinct_integral_data(con, *param)
    print('Done.')


def generate_unique_groups(group_cols, group_count):
    group_unique_counts = []
    num_group_cols = len(group_cols)
    remaining_group_count = group_count
    for i in range(num_group_cols):
        assert(len(group_cols[i]) == 2)
        group_type = group_cols[i][0]
        group_weight = group_cols[i][1]
        assert(group_weight >= 0 and group_weight <= 1)
        remaining_group_cols = num_group_cols - i
        group_pow = (1 / remaining_group_cols) ** (1 - group_weight)
        assert(group_pow >= 0 and group_pow <= 1)
        group_unique_count = math.ceil(remaining_group_count ** group_pow)
        remaining_group_count /= group_unique_count
        group_unique_counts.append(group_unique_count)
    return f"""\rSELECT row_number() OVER () AS group_id, *
               \rFROM{f', '.join([f" (SELECT c0 AS c{i} FROM '{SOURCE_DATA_DIR}/{t}s.parquet' USING SAMPLE {c}) t{i}" for i, (t, c) in enumerate(zip([gc[0] for gc in group_cols], group_unique_counts))])}
               \rLIMIT {group_count}
           """

def generate_dataset(con, file, group_cols, group_count, total_count, power, test_run=True):
    group_ids = np.arange(group_count)
    distribution = (np.random.power(power, total_count) * group_count).astype(int)
    occurrences_df = pd.DataFrame(np.take(group_ids, distribution), columns=['group_id'])
    
    if test_run:
        print(con.execute("""
        WITH counts_per_group AS (
            SELECT group_id, count(*) count
            FROM occurrences_df
            GROUP BY group_id
        )
        SELECT min(count) AS mi,
               max(count) AS ma,
               median(count) AS med,
               avg(count) AS avg,
               sum(count) AS s,
               count(*) AS groups
        from counts_per_group
        """).fetchdf())
        
        print("Is the generated distribution OK? [y/n]")
        a = input()
        if a == 'n':
            return
        elif a != 'y':
            print("Input must be y/n!")
    
    # Create groups
    q = generate_unique_groups(group_cols, group_count)
    con.execute(f"CREATE OR REPLACE VIEW groups AS ({q})")
    
    # Join them
    con.execute(f"""
    COPY (
        SELECT groups.* EXCLUDE (group_id)
        FROM groups
        JOIN occurrences_df
        USING (group_id)
    ) TO '{file}'
    """)


def generate_group_data():
    if not os.path.exists(GROUPS_DATA_DIR):
        os.mkdir(GROUPS_DATA_DIR)

    con = duckdb.connect()
    config = load_config(con)
    configs = permute_dicts(config)

    print('Generating group data ...')
    for c in tqdm.tqdm(configs):
        name = conf_to_str(c)
        file_name = f'{GROUPS_DATA_DIR}/{name}.csv'
        if not os.path.exists(file_name):
            group_cols = [(c['type'], 0) for _ in range(c['column_count'])]
            generate_dataset(con, file_name, group_cols, c['group_count'], c['total_count'], c['power'], False)
    print('Done.')


def main():
    if not os.path.exists(DATA_DIR):
        os.mkdir(DATA_DIR)
    
    generate_source_data()
    generate_group_data()


if __name__ == '__main__':
    main()
