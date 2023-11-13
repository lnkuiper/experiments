import os
import sys
import psycopg2


SYSTEM_DIR = os.path.dirname(__file__)
sys.path.append(f'{SYSTEM_DIR}/..')
from util.util import *


def run_query(query, cur):
    cur.execute(query)
    return cur.fetchall()


def main():
    con = psycopg2.connect(database="mydb", host="localhost", user="postgres", password="mysecretpassword", port=5432)
    cur = con.cursor()
    run_benchmark('umbra', run_query, cur)


if __name__ == '__main__':
    main()
