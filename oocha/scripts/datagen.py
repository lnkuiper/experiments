import os
import duckdb
import sys
import tqdm


SCRIPTS_DIR = os.path.dirname(__file__)
sys.path.append(f'{SCRIPTS_DIR}/..')
from benchmark.util.util import *


TEMPORARY_DB = 'temporary.db'


def generate_sf(sf):
    if os.path.exists(TEMPORARY_DB):
        os.remove(TEMPORARY_DB)

    csv = get_csv_path(sf)
    if not os.path.exists(csv):
        con = duckdb.connect(TEMPORARY_DB)
        con.execute(f"""CALL dbgen(sf={sf});""")
        con.execute(f"""COPY lineitem TO '{DATA_DIR}/lineitem_sf{sf}.csv';""")
        con.close()

        os.remove(TEMPORARY_DB)


def main():
    if not os.path.exists(DATA_DIR):
        os.mkdir(DATA_DIR)

    for sf in SCALE_FACTORS:
        generate_sf(sf)


if __name__ == '__main__':
    main()
