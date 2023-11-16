import os
import sys
import subprocess
from tableauhyperapi import HyperProcess, Telemetry, Connection, CreateMode


SYSTEM_DIR = os.path.dirname(__file__)
sys.path.append(f'{SYSTEM_DIR}/..')
from util.util import *


def main():
    hyper_path = f'{SYSTEM_DIR}/hyper/'
    db_path = f"{SYSTEM_DIR}/hyper/mydb"
    process_parameters = {"default_database_version": "2"}
    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, hyper_path=hyper_path, parameters=process_parameters) as hyper:
        with Connection(hyper.endpoint, db_path, CreateMode.CREATE_IF_NOT_EXISTS) as con:
            for sf in SCALE_FACTORS:
                con.execute_query("""START TRANSACTION;""").close()
                con.execute_query(get_schema(sf)).close()
                rows = con.execute_list_query(query=f"""SELECT count(*) FROM lineitem{sf};""")
               if res[0][0] == 0:
                   print(f'Loading hyper SF{sf} ...')
                   con.execute_query(f"""COPY lineitem{sf} FROM '{get_csv_path(sf)}' (FORMAT CSV, HEADER TRUE, QUOTE '"', DELIMITER ',');""").close()
                   print(f'Loading hyper SF{sf} done.')
               con.execute_query("""COMMIT;""").close()


if __name__ == '__main__':
    main()
