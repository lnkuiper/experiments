import os
import sys
import psycopg2
import subprocess


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


UMBRA_FUNCTIONS = {
    'query': query_fun,
    'close': close_fun,
    'already_loaded': already_loaded_fun,
    'load': load_fun,
    'drop': drop_fun,
}


def main():
    db_path = f'{SYSTEM_DIR}/mydb.umbra'
    if not os.path.exists(db_path):
        subprocess.run(f'{SYSTEM_DIR}/umbra/bin/sql --createdb {db_path} {SYSTEM_DIR}/create-role.sql'.split(' '))
        subprocess.run(f'chmod 777 {db_path}'.split(' '))
    server = subprocess.Popen(f'{SYSTEM_DIR}/umbra/bin/server -address=127.0.0.1 -port=5433 {db_path}'.split(' '), stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    time.sleep(10)

    con = psycopg2.connect(host="localhost", user="postgres", password="mysecretpassword", port=5433)
    cur = con.cursor()
    cur.execute("""ROLLBACK;""")
    run_experiments('umbra', UMBRA_FUNCTIONS, con)


if __name__ == '__main__':
    main()
