import os
import sys
import psycopg2
import subprocess


SYSTEM_DIR = os.path.dirname(__file__)
sys.path.append(f'{SYSTEM_DIR}/..')
from util.util import *


def main():
    server = subprocess.Popen(f'{SYSTEM_DIR}/hyper/hyperd --database ./hyper/mydb --log-dir . --skip-license --init-user raasveld --listen-connection tab.tcp://localhost:7484,tab.domain:///tmp/LD/domain/.s.PGSQL.7484 --no-password run'.split(' '))
    time.sleep(10)
    try:
        con = psycopg2.connect(database=f"{SYSTEM_DIR}/hyper/mydb", host="localhost", user="raasveld", password="", port=7484)
        cur = con.cursor()
    
        for sf in SCALE_FACTORS:
            cur.execute("""START TRANSACTION;""")
            cur.execute(get_schema(sf))
            cur.execute(f"""SELECT count(*) FROM lineitem{sf};""")
            if cur.fetchall()[0][0] == 0:
                print(f'Loading hyper SF{sf} ...')
                cur.execute(f"""COPY lineitem{sf} FROM '{get_csv_path(sf)}' (FORMAT CSV, HEADER TRUE, QUOTE '"', DELIMITER ',');""")
                print(f'Loading hyper SF{sf} done.')
            cur.execute("""COMMIT""")
    except Exception as e:
        my_exception = e
    finally:
        server.terminate()
    raise my_exception


if __name__ == '__main__':
    main()
