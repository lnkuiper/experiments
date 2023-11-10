import os
import sys


SYSTEM_DIR = os.path.dirname(__file__)
sys.path.append(f'{SYSTEM_DIR}/..')
from util.util import *


def main():
    con = duckdb.connect(f'{SYSTEM_DIR}/data.db')
    con.execute("""PRAGMA enable_progress_bar;""")
    con.execute("""SET preserve_insertion_order=false;""")
    for sf in SCALE_FACTORS:
        con.execute(get_schema(sf))
        if con.execute(f"""SELECT count(*) FROM lineitem{sf};""").fetchall()[0][0] == 0:
            print(f'Loading duckdb SF{sf} ...')
            con.execute(f"""COPY lineitem{sf} FROM '{get_csv_path(sf)}' (HEADER TRUE);""")
            print(f'Loading duckdb SF{sf}.')


if __name__ == '__main__':
    main()
