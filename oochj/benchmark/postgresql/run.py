import os
import sys
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


SYSTEM_DIR = os.path.dirname(__file__)
sys.path.append(f'{SYSTEM_DIR}/..')
from util.util import *


def query_fun(query, cur):
    cur.execute(query)
    return cur.fetchall()


def close_fun(res, cur):
    del res


def already_loaded_fun(row_count, cur):
    table_name = row_count_to_table_name(row_count)
    try:
        return cur.execute(f"SELECT count(*) FROM {table_name};").fetchall()[0][0] == row_count
    except:
        return False


def load_fun(row_count, cur):
    table_name = row_count_to_table_name(row_count)
    cur.execute("START TRANSACTION;")
    cur.execute(TABLE_SCHEMA.replace('%TABLE_NAME%', table_name))
    for file in row_count_to_file_list(row_count):
        cur.execute(f"""COPY {table_name} FROM '{file}' (FORMAT CSV, QUOTE '"', DELIMITER ',', HEADER TRUE);""")
    cur.execute("COMMIT;")


def drop_fun(row_count, cur):
    table_name = row_count_to_table_name(row_count)
    cur.execute(f"DROP TABLE IF EXISTS {table_name};")


POSTGRESQL_FUNCTIONS = {
    'query': query_fun,
    'close': close_fun,
    'already_loaded': already_loaded_fun,
    'load': load_fun,
    'drop': drop_fun,
}


def main():
    if True:
        con = psycopg2.connect(database="postgres", host="localhost", user="ubuntu", password="secret", port=5432)
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = con.cursor()
        cur.execute("DROP DATABASE IF EXISTS mydb;")
        cur.execute("CREATE DATABASE mydb;")
        cur.close()
        con.close()

    con = psycopg2.connect(database="mydb", host="localhost", user="ubuntu", password="secret", port=5432)
    cur = con.cursor()

    cur.execute("ROLLBACK;")
    cur.execute("DROP TABLESPACE IF EXISTS temp_space;")
    cur.execute("CREATE TABLESPACE temp_space LOCATION '/data/experiments/oochj/benchmark/postgresql/temp';")
    cur.execute("SET temp_tablespaces TO 'temp_space';")

    run_experiments('postgresql', POSTGRESQL_FUNCTIONS, cur)


if __name__ == '__main__':
    main()
