import os
import sys


SYSTEM_DIR = os.path.dirname(__file__)
sys.path.append(f'{SYSTEM_DIR}/..')
from util.util import *


def run_query(query, con):
    return con.sql(query).fetchall()


def main():
    con = duckdb.connect(f'/data/data.db', read_only=True)
    con.execute("""SET preserve_insertion_order=false""")
    con.execute("""SET memory_limit='20GB'""")
    run_benchmark('duckdb', run_query, con)


if __name__ == '__main__':
    main()
