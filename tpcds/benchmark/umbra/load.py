import os
import sys
import psycopg2
import subprocess


SYSTEM_DIR = os.path.dirname(__file__)
sys.path.append(f'{SYSTEM_DIR}/..')
from util.util import *


def main():
    con = psycopg2.connect(host="localhost", user="postgres", password="postgres", port=5432, dbname="postgres")
    cur = con.cursor()
    for sf in SCALE_FACTORS:
        print(f'Loading Umbra SF{sf} ...')
        cur.execute('ROLLBACK;')
        cur.execute('START TRANSACTION;')
        for q in get_schema(sf).split(';'):
            if not q.strip():
                continue;
            cur.execute(f"{q};")
        for q in get_load(sf).split(';'):
            if not q.strip():
                continue
            cur.execute(f"{q};")
        cur.execute('COMMIT;')


if __name__ == '__main__':
    main()
