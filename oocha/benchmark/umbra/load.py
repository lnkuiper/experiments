import os
import sys
import psycopg2


SYSTEM_DIR = os.path.dirname(__file__)
sys.path.append(f'{SYSTEM_DIR}/..')
from util.util import *


def main():
    con = psycopg2.connect(database="postgres", host="localhost", user="postgres", password="mysecretpassword", port=5432)
    cur = con.cursor()
    for sf in SCALE_FACTORS:
        cur.execute("""START TRANSACTION;""")
        cur.execute(get_schema(sf))
        cur.execute(f"""SELECT count(*) FROM lineitem{sf};""")
        if cur.fetchall()[0][0] == 0:
            print(f'Loading umbra SF{sf} ...')
            cur.execute(f"""COPY lineitem{sf} FROM '{get_csv_path(sf)}' (FORMAT CSV, HEADER TRUE, QUOTE '"', DELIMITER ',');""")
            print(f'Loading umbra SF{sf} done.')
        cur.execute("""COMMIT""")


if __name__ == '__main__':
    main()
