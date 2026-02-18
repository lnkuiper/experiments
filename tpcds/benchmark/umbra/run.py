import os
import sys
import psycopg2
import subprocess


SYSTEM_DIR = os.path.dirname(__file__)
sys.path.append(f'{SYSTEM_DIR}/..')
from util.util import *


def schema_fun(sf, con):
    return


def query_fun(query, cur):
    cur.execute(query)
    return cur.fetchall()


def close_fun(res):
    del res


def main():
    con = psycopg2.connect(host="localhost", user="postgres", password="postgres", port=5432, dbname="postgres")
    cur = con.cursor()
    cur.execute("""ROLLBACK;""");
    run_benchmark('umbra', schema_fun, query_fun, close_fun, cur)

if __name__ == '__main__':
    main()
