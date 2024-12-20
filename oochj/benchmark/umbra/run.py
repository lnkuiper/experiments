import os
import sys
import psycopg2
import subprocess
import signal


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
        cur.execute(f"SELECT count(*) FROM {table_name};")
        res = cur.fetchall()
        return res[0][0] == row_count
    except Exception as e:
        return False


def load_fun(row_count, cur):
    table_name = row_count_to_table_name(row_count)
    cur.execute("START TRANSACTION;")
    cur.execute(TABLE_SCHEMA.replace('%TABLE_NAME%', table_name).replace(';', 'WITH (storage=columnar);'))
    for file in row_count_to_file_list(row_count):
        cur.execute(f"""COPY {table_name} FROM '{file}' (FORMAT CSV, QUOTE '"', DELIMITER ',', HEADER TRUE);""")
    cur.execute("COMMIT;")


def drop_fun(row_count, cur):
    table_name = row_count_to_table_name(row_count)
    cur.execute(f"DROP TABLE IF EXISTS {table_name};")


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
    run_experiments('umbra', UMBRA_FUNCTIONS, cur)

    os.killpg(os.getpgid(server.pid), signal.SIGTERM)


if __name__ == '__main__':
    main()
