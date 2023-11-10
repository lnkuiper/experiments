import os
import sys


SYSTEM_DIR = os.path.dirname(__file__)
sys.path.append(f'{SYSTEM_DIR}/..')
from util.util import *


def main():
    for sf in SCALE_FACTORS:
        print(f'Loading SF{sf} into DuckDB ...')
        con = duckdb.connect(f'{SYSTEM_DIR}/sf{sf}.db')
        con.execute(get_schema())

        if con.execute("""SELECT count(*) FROM lineitem;""").fetchall()[0][0] == 0:
            con.execute("""PRAGMA enable_progress_bar;""")
            con.execute(f"""COPY lineitem FROM '{get_csv_path(sf)}' (HEADER TRUE);""")
            
        con.close()
        print('Done.')


if __name__ == '__main__':
    main()
