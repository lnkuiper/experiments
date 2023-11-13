import os
import sys
import psycopg2


SYSTEM_DIR = os.path.dirname(__file__)
sys.path.append(f'{SYSTEM_DIR}/..')
from util.util import *


def run_query(query, cur):
    cur.execute(query)
    # return cur.fetchall()


def main():
    con = psycopg2.connect(database=f"{SYSTEM_DIR}/hyper/mydb", host="localhost", user="raasveld", password="", port=7484)
    cur = con.cursor()
    run_benchmark('hyper', run_query, cur)


if __name__ == '__main__':
    main()
