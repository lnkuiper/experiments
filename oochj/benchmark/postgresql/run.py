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


def close_fun(res):
    del res


def main():
    con = psycopg2.connect(database="mydb", host="localhost", user="ubuntu", password="secret", port=5432)
    cur = con.cursor()

    #cur.execute("SET default_tablespace=mytablespace;")
    #cur.execute("SET temp_tablespaces='mytablespace';")
    cur.execute("ROLLBACK;")
    cur.execute("DROP TABLESPACE IF EXISTS temp_space;")
    cur.execute("CREATE TABLESPACE temp_space LOCATION '/data/experiments/oochj/benchmark/postgresql/temp';")
    cur.execute("SET temp_tablespaces TO 'temp_space';")

    run_benchmark('postgresql', schema_fun, query_fun, close_fun, cur)


if __name__ == '__main__':
    main()
