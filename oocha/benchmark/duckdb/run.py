import os
import sys


SYSTEM_DIR = os.path.dirname(__file__)
sys.path.append(f'{SYSTEM_DIR}/..')
from util.util import *


def run_query(query, benchmark_con):
    benchmark_con.execute(query).fetchall()


def main():
    print('Running DuckDB ...')
    for sf in SCALE_FACTORS:
        print(f'Running SF{sf} ...')
        benchmark_con = duckdb.connect(f'{SYSTEM_DIR}/sf{sf}.db', read_only=True)
        run_benchmark('duckdb', run_query, benchmark_con)
        print(f'SF{sf} done.')
    print('DuckDB done.')


if __name__ == '__main__':
    main()
