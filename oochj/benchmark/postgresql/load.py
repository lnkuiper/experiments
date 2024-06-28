import os
import sys
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


SYSTEM_DIR = os.path.dirname(__file__)
sys.path.append(f'{SYSTEM_DIR}/..')
from util.util import *


def main():
    con = psycopg2.connect(database="postgres", host="localhost", user="laurens", password="", port=5432)
    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()
    cur.execute("DROP DATABASE IF EXISTS mydb;")
    cur.execute("CREATE DATABASE mydb;")
    cur.close()
    con.close()

    con = psycopg2.connect(database="mydb", host="localhost", user="laurens", password="", port=5432)
    cur = con.cursor()

    cur.execute("CREATE TABLESPACE mytablespace LOCATION '/data/postgresql';")
    cur.execute("SET default_tablespace=mytablespace;")
    cur.execute("SET temp_tablespaces='mytablespace';")

    for sf in SCALE_FACTORS:
        cur.execute("START TRANSACTION;")
        cur.execute(f"CREATE SCHEMA sf{sf};")
        cur.execute(f"SET search_path=sf{sf};")
        print(f'Loading postgresql SF{sf} ...')
        cur.execute(get_schema(sf))
        cur.execute(get_load(sf))
        print(f'Loading postgresql SF{sf} done.')
        cur.execute("COMMIT;")


if __name__ == '__main__':
    main()
