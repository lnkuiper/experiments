import os
import sys


SYSTEM_DIR = os.path.dirname(__file__)
sys.path.append(f'{SYSTEM_DIR}/..')
from util.util import *


def main():
	db_path = f'{SYSTEM_DIR}/mydb.duckdb'
    con = duckdb.connect(db_path)

    con.execute("SET allocator_background_threads=true;")

    # base table with all data
    con.execute(TABLE_SCHEMA.replace('%TABLE_NAME%', 'r1000M'))
    con.execute(f"COPY r1000M FROM '{DATA_DIR}/*.csv';")

    default_config = get_config('default')
    build_row_count = default_config['build']['row_count']
    probe_row_count = default_config['probe']['row_count']

    # default build table
    con.execute(TABLE_SCHEMA.replace('%TABLE_NAME%', row_count_to_table_name(build_row_count)))
    con.execute(f"INSERT INTO r200M SELECT * FROM r1000M LIMIT {build_row_count};")

    # default probe table
    con.execute(TABLE_SCHEMA.replace('%TABLE_NAME%', row_count_to_table_name(probe_row_count)))
    con.execute(f"INSERT INTO r200M SELECT * FROM r1000M LIMIT {probe_row_count};")


if __name__ == '__main__':
	main()
