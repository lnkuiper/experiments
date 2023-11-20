import os
import sys
import psycopg2
import subprocess


SYSTEM_DIR = os.path.dirname(__file__)
sys.path.append(f'{SYSTEM_DIR}/..')
from util.util import *


def main():
    #db_path = f'{SYSTEM_DIR}/mydb'
    db_path = '/data/umbra/mydb.umbra'
    if not os.path.exists(db_path):
        subprocess.run(f'{SYSTEM_DIR}/umbra/bin/sql --createdb {db_path} {SYSTEM_DIR}/create-role.sql'.split(' '))
        subprocess.run(f'chmod 777 {db_path}'.split(' '))
    server = subprocess.Popen(f'{SYSTEM_DIR}/umbra/bin/server -address=127.0.0.1 -port=5433 {db_path}'.split(' '), stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    time.sleep(10)
    try:
        con = psycopg2.connect(host="localhost", user="postgres", password="mysecretpassword", port=5433)
        cur = con.cursor()
        for sf in SCALE_FACTORS:
            #cur.execute("""START TRANSACTION;""")
            cur.execute(get_schema(sf))
            cur.execute(f"""SELECT count(*) FROM lineitem{sf};""")
            if cur.fetchall()[0][0] == 0:
                print(f'Loading umbra SF{sf} ...')
                cur.execute(f"""COPY lineitem{sf} FROM '{get_csv_path(sf)}' (FORMAT CSV, HEADER TRUE, QUOTE '"', DELIMITER ',');""")
                print(f'Loading umbra SF{sf} done.')
            #cur.execute("""COMMIT""")
    except Exception as e:
        my_exception = e
    finally:
        server.terminate()
    raise my_exception


if __name__ == '__main__':
    main()
