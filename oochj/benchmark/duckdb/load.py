import os
import sys


SYSTEM_DIR = os.path.dirname(__file__)
sys.path.append(f'{SYSTEM_DIR}/..')
from util.util import *


def main():
    db_path = f'{SYSTEM_DIR}/mydb.duckdb'
    # db_path = '/data/duckdb/mydb.duckdb'
    if os.path.exists(db_path):
        os.remove(db_path)
    con = duckdb.connect(db_path, read_only=True)
    con.execute("SET preserve_insertion_order=false;")
    # con.execute("SET allocator_background_threads=true;")
    for sf in SCALE_FACTORS:
        con.execute(f"CREATE OR REPLACE SCHEMA sf{sf};")
        con.execute(f"USE sf{sf};")
        print(f'Loading duckdb SF{sf} ...')
        con.execute(get_schema(sf))
        con.execute(get_load(sf))
        print(f'Loading duckdb SF{sf} done.')


if __name__ == '__main__':
    main()
