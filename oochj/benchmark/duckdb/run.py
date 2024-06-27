import os
import sys


SYSTEM_DIR = os.path.dirname(__file__)
sys.path.append(f'{SYSTEM_DIR}/..')
from util.util import *


def schema_fun(sf, con):
    con.execute(f"USE sf{sf};")


def query_fun(query, con):
    return con.sql(query).fetchall()


def main():
    db_path = f'{SYSTEM_DIR}/data.db'
    # db_path = '/data/data.db'
    con = duckdb.connect(db_path, read_only=True)

    con.execute("SET preserve_insertion_order=false;")
    # con.execute("SET allocator_background_threads=true;")

    run_benchmark('duckdb', schema_fun, query_fun, con)


if __name__ == '__main__':
    main()
