import os
import sys
from tableauhyperapi import HyperProcess, Telemetry, Connection, CreateMode


SYSTEM_DIR = os.path.dirname(__file__)
sys.path.append(f'{SYSTEM_DIR}/..')
from util.util import *


def run_query(query, con):
    con.execute_query(query).close()


def main():
    hyper_path = f'{SYSTEM_DIR}/hyper/'
    #db_path = f"{SYSTEM_DIR}/hyper/mydb"
    db_path = '/data/hyper/mydb'
    process_parameters = {"default_database_version": "2"}
    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, hyper_path=hyper_path, parameters=process_parameters) as hyper:
        with Connection(hyper.endpoint, db_path, CreateMode.CREATE_IF_NOT_EXISTS) as con:
            run_benchmark('hyper', run_query, con)


if __name__ == '__main__':
    main()
