import os
import sys


SYSTEM_DIR = os.path.dirname(__file__)
sys.path.append(f'{SYSTEM_DIR}/..')
from util.util import *


def query_fun(query, con):
    return con.sql(query).fetchall()


def close_fun(res, con):
    del res


def already_loaded_fun(table_name, con):
    try:
        return con.execute(f"SELECT count(*) FROM {table_name}").fetchall()[0][0] != 0
    except:
        return False

def load_fun(table_schema, filename, table_name, con):
    table_schema = table_schema.replace('%TABLE_NAME%', table_name)
    con.execute("START TRANSACTION;")
    con.execute(f"{table_schema}")
    con.execute(f"""COPY "{table_name}" FROM '{filename}';""")
    con.execute("COMMIT;")


def create_view_fun(table_alias, table_name, con):
    con.execute(f"""CREATE OR REPLACE VIEW {table_alias} AS SELECT * FROM "{table_name}";""")


DUCKDB_FUNCTIONS = {
    'query': query_fun,
    'close': close_fun,
    'already_loaded': already_loaded_fun,
    'load': load_fun,
    'create_view': create_view_fun,
}


def main():
    db_path = f'{SYSTEM_DIR}/mydb.duckdb'
    con = duckdb.connect(db_path)

    con.execute("SET preserve_insertion_order=false;")
    con.execute("SET memory_limit='23.2GiB'")
    # con.execute("SET allocator_background_threads=true;")

    run_experiments('duckdb', DUCKDB_FUNCTIONS, con)


if __name__ == '__main__':
    main()
