import os
import sys


SYSTEM_DIR = os.path.dirname(__file__)
sys.path.append(f'{SYSTEM_DIR}/..')
from util.util import *


def run_query(query, con):
    return con.execute(query).fetchall()


def main():
    con = duckdb.connect(f'{SYSTEM_DIR}/data.db', read_only=True)
    run_benchmark('duckdb', run_query, con)


if __name__ == '__main__':
    main()
