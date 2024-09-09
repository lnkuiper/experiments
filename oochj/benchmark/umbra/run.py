import os
import sys
import psycopg2
import subprocess


SYSTEM_DIR = os.path.dirname(__file__)
sys.path.append(f'{SYSTEM_DIR}/..')
from util.util import *


def schema_fun(sf, cur):
    cur.execute(f"SET search_path=sf{sf};")


def query_fun(query, cur):
    cur.execute(query)
    return cur.fetchall()


def close_fun(res):
    del res


def main():
    db_path = f'{SYSTEM_DIR}/mydb.umbra'
    #db_path = '/data/umbra/mydb.umbra'
    server = subprocess.Popen(f'{SYSTEM_DIR}/umbra/bin/server -address=127.0.0.1 -port=5433 {db_path}'.split(' '), stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    time.sleep(10)
    my_exception = None
    try:
        con = psycopg2.connect(host="localhost", user="postgres", password="mysecretpassword", port=5433)
        cur = con.cursor()
        cur.execute("""ROLLBACK;""");
        run_benchmark('umbra', schema_fun, query_fun, close_fun, cur)
    except Exception as e:
        my_exception = e
    finally:
        server.terminate()
        if my_exception:
            raise my_exception

if __name__ == '__main__':
    main()
