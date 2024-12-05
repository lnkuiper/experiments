import os
import sys


SYSTEM_DIR = os.path.dirname(__file__)
sys.path.append(f'{SYSTEM_DIR}/..')
from util.util import *


def query_fun(query, con):
    return con.sql(query).fetchall()


def close_fun(res, con):
    del res


def already_loaded_fun(row_count, con):
    table_name = row_count_to_table_name(row_count)
    try:
        return con.execute(f"SELECT count(*) FROM {table_name}").fetchall()[0][0] == row_count
    except:
        return False


def load_fun(row_count, con):
    table_name = row_count_to_table_name(row_count)
    con.execute("START TRANSACTION;")
    con.execute(TABLE_SCHEMA.replace('%TABLE_NAME%', table_name))
    con.execute(f"INSERT INTO {table_name} SELECT * FROM r1000M LIMIT {row_count};")
    con.execute("COMMIT;")


DUCKDB_FUNCTIONS = {
    'query': query_fun,
    'close': close_fun,
    'already_loaded': already_loaded_fun,
    'load': load_fun,
}


def main():
    db_path = f'{SYSTEM_DIR}/mydb.duckdb'
    con = duckdb.connect(db_path)

    con.execute("SET preserve_insertion_order=false;")
    con.execute("SET memory_limit='23.2GiB'")
    con.execute("SET allocator_background_threads=true;")

    run_experiments('duckdb', DUCKDB_FUNCTIONS, con)


if __name__ == '__main__':
    main()
