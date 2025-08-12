import os
import sys
from tableauhyperapi import HyperProcess, Telemetry, Connection, CreateMode


SYSTEM_DIR = os.path.dirname(__file__)
sys.path.append(f'{SYSTEM_DIR}/..')
from util.util import *


def main():
    db_path = f"{SYSTEM_DIR}/mydb.hyper"
    # db_path = '/data/hyper/mydb.hyper'
    if os.path.exists(db_path):
        os.remove(db_path)

    process_parameters = {"default_database_version": "2"}
    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, parameters=process_parameters) as hyper:
        with Connection(hyper.endpoint, db_path, CreateMode.CREATE_IF_NOT_EXISTS) as con:
            for sf in SCALE_FACTORS:
                con.execute_query("START TRANSACTION;").close()
                con.execute_query(f"CREATE SCHEMA sf{sf};").close()
                con.execute_query(f"SET search_path=sf{sf};").close()
                print(f'Loading hyper SF{sf} ...')
                for q in get_schema(sf).split(';'):
                    con.execute_query(f"{q};").close()
                for q in get_load(sf).split(';'):
                    con.execute_query(f"{q};").close()
                print(f'Loading hyper SF{sf} done.')
                con.execute_query("COMMIT;").close()


if __name__ == '__main__':
    main()
