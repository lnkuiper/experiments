import os
import sys
import psycopg2


SYSTEM_DIR = os.path.dirname(__file__)
sys.path.append(f'{SYSTEM_DIR}/..')
from util.util import *


def query_fun(query, cur):
    cur.execute(query)
    return cur.fetchall()


def close_fun(res):
    del res


def already_loaded_fun(row_count, cur):
    table_name = row_count_to_table_name(row_count)
    try:
        return cur.execute(f"SELECT count(*) FROM {table_name};").fetchall()[0][0] == row_count
    except:
        return False


def load_fun(row_count, cur):
    table_name = row_count_to_table_name(row_count)
    con.execute("START TRANSACTION;")
    con.execute(TABLE_SCHEMA.replace('%TABLE_NAME%', table_name))
    for file in row_count_to_file_list(row_count):
        con.execute(f"COPY {table_name} FROM '{file}';")
    con.execute("COMMIT;")


def drop_fun(row_count, cur):
    table_name = row_count_to_table_name(row_count)
    con.execute(f"DROP TABLE IF EXISTS {table_name};")


POSTGRESQL_FUNCTIONS = {
    'query': query_fun,
    'close': close_fun,
    'already_loaded': already_loaded_fun,
    'load': load_fun,
    'drop': drop_fun,
}


def main():
    con = psycopg2.connect(database="mydb", host="localhost", user="ubuntu", password="secret", port=5432)
    cur = con.cursor()

    cur.execute("ROLLBACK;")
    cur.execute("DROP TABLESPACE IF EXISTS temp_space;")
    cur.execute("CREATE TABLESPACE temp_space LOCATION '/data/experiments/oochj/benchmark/postgresql/temp';")
    cur.execute("SET temp_tablespaces TO 'temp_space';")

    run_experiments('postgresql', POSTGRESQL_FUNCTIONS, con)


if __name__ == '__main__':
    main()
