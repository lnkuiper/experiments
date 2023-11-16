import os
import sys
import psycopg2
import subprocess
from tableauhyperapi import HyperProcess, Telemetry, Connection, CreateMode


SYSTEM_DIR = os.path.dirname(__file__)
sys.path.append(f'{SYSTEM_DIR}/..')
from util.util import *


def main():
    hyper_path = f'{SYSTEM_DIR}/hyper/'
    db_path = f"{SYSTEM_DIR}/hyper/mydb"
    if not os.path.exists(db_path):
        process_parameters = {"default_database_version": "2"}
        with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, hyper_path=hyper_path, parameters=process_parameters) as hyper:
            with Connection(hyper.endpoint, db_path, CreateMode.CREATE) as connection:
                connection.execute_query("""SELECT 1+3""")

    server = subprocess.Popen(f'{hyper_path}/hyperd --database ./hyper/mydb --log-dir . --skip-license --init-user ubuntu --listen-connection tab.tcp://localhost:7484,tab.domain:///tmp/LD/domain/.s.PGSQL.7484 --no-password run'.split(' '), stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    time.sleep(10)
    try:
        con = psycopg2.connect(host="localhost", user="ubuntu", password="", port=7484)
        cur = con.cursor()
        cur.execute("""START TRANSACTION;""")
        cur.execute("""CREATE SCHEMA main;""")
        cur.execute("""COMMIT;""")
        cur.execute("""SET search_path TO main""")
    
        for sf in SCALE_FACTORS:
            cur.execute("""START TRANSACTION;""")
            cur.execute(get_schema(sf, system='hyper'))
            cur.execute(f"""SELECT count(*) FROM lineitem{sf};""")
            if cur.fetchall()[0][0] == 0:
                print(f'Loading hyper SF{sf} ...')
                cur.execute(f"""COPY lineitem{sf} FROM '{get_csv_path(sf)}' (FORMAT CSV, HEADER TRUE, QUOTE '"', DELIMITER ',');""")
                print(f'Loading hyper SF{sf} done.')
            cur.execute("""COMMIT;""")
    except Exception as e:
        my_exception = e
    finally:
        server.terminate()
    raise my_exception


if __name__ == '__main__':
    main()
