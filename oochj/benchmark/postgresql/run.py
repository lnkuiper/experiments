import os
import sys
import psycopg2


SYSTEM_DIR = os.path.dirname(__file__)
sys.path.append(f'{SYSTEM_DIR}/..')
from util.util import *


def schema_fun(sf, cur):
    cur.execute(f"SET search_path=sf{sf};")


def query_fun(query, cur):
    cur.execute(query)
    return cur.fetchall()


def main():
    con = psycopg2.connect(database="mydb", host="localhost", user="", password="", port=5432)
    cur = con.cursor()

    cur.execute("SET default_tablespace=mytablespace;")
    cur.execute("SET temp_tablespaces='mytablespace';")

    run_benchmark('postgresql', schema_fun, query_fun, cur)


if __name__ == '__main__':
    main()
