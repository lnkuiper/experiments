import os
import sys


SYSTEM_DIR = os.path.dirname(__file__)
sys.path.append(f'{SYSTEM_DIR}/..')
from util.util import *

# DB_DIR = '~'
DB_DIR = '/data'


def schema_fun(sf, con):
    return con.execute(f"ATTACH OR REPLACE '{DB_DIR}/tpcds_sf{sf}.duckdb' AS tpcds_sf{sf} (READ_ONLY); USE tpcds_sf{sf};")


def query_fun(query, con):
    return con.sql(query).fetchall()


def close_fun(res):
    del res


def main():
    con = duckdb.connect()

    con.execute("SET preserve_insertion_order=false;")
    con.execute("SET allocator_background_threads=true;")
    con.execute(f"SET temp_directory='{DB_DIR}/tmp';")

    run_benchmark('duckdb', schema_fun, query_fun, close_fun, con)


if __name__ == '__main__':
    main()
