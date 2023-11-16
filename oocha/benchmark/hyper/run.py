import os
import sys
import psycopg2
import subprocess


SYSTEM_DIR = os.path.dirname(__file__)
sys.path.append(f'{SYSTEM_DIR}/..')
from util.util import *


def run_query(query, cur):
    cur.execute(query)
    return cur.fetchall()


def main():
    server = subprocess.Popen(f'{SYSTEM_DIR}/hyper/hyperd --database ./hyper/mydb --log-dir . --skip-license --init-user raasveld --listen-connection tab.tcp://localhost:7484,tab.domain:///tmp/LD/domain/.s.PGSQL.7484 --no-password run'.split(' '), stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    time.sleep(10)
    try:
        con = psycopg2.connect(database=f"{SYSTEM_DIR}/hyper/mydb", host="localhost", user="raasveld", password="", port=7484)
        cur = con.cursor()
        run_benchmark('hyper', run_query, cur)
    except Exception as e:
        my_exception = e
    finally:
        server.terminate()
    raise my_exception



if __name__ == '__main__':
    main()
