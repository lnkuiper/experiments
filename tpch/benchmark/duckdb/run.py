import os
import sys


SYSTEM_DIR = os.path.dirname(__file__)
sys.path.append(f'{SYSTEM_DIR}/..')
from util.util import *


def schema_fun(sf, con):
    return None #con.execute(f"USE sf{sf};").fetchall()


def query_fun(query, con):
    return con.sql(query).fetchall()


def close_fun(res):
    del res


def main():
    db_path = f'{SYSTEM_DIR}/mydb.duckdb'
    # db_path = '/data/mydb.duckdb'
    con = duckdb.connect(db_path, read_only=True)

    con.execute("SET preserve_insertion_order=false;")
    con.execute("SET allocator_background_threads=true;")
    #con.execute("SET disabled_optimizers TO 'compressed_materialization';")

    run_benchmark('duckdb', schema_fun, query_fun, close_fun, con)


if __name__ == '__main__':
    main()
