import os
import sys
from tableauhyperapi import HyperProcess, Telemetry, Connection, CreateMode


SYSTEM_DIR = os.path.dirname(__file__)
sys.path.append(f'{SYSTEM_DIR}/..')
from util.util import *


def query_fun(query, con):
    return con.execute_query(query)


def close_fun(res, con):
    res.close()
    del res


def already_loaded_fun(row_count, con):
    table_name = row_count_to_table_name(row_count)
    loaded = False
    res = None
    try:
        res = con.execute_query(f"SELECT count(*) FROM {table_name};")
        res.next_row()
        loaded = res.get_value(0) == row_count
        if not loaded:
            con.execute_query(f"DROP TABLE {table_name};").close()
    except:
        None
    if res:
        res.close()
    return loaded


def load_fun(row_count, con):
    table_name = row_count_to_table_name(row_count)
    con.execute_query("START TRANSACTION;").close()
    con.execute_query(TABLE_SCHEMA.replace('%TABLE_NAME%', table_name)).close()
    for file in row_count_to_file_list(row_count):
        con.execute_query(f"COPY {table_name} FROM '{file}' (FORMAT CSV, HEADER TRUE);").close()
    con.execute_query("COMMIT;").close()


def drop_fun(row_count, con):
    table_name = row_count_to_table_name(row_count)
    con.execute_query(f"DROP TABLE IF EXISTS {table_name};").close()


HYPER_FUNCTIONS = {
    'query': query_fun,
    'close': close_fun,
    'already_loaded': already_loaded_fun,
    'load': load_fun,
    'drop': drop_fun,
}


def main():
    hyper_path = f'{SYSTEM_DIR}/hyper/'
    db_path = f"{SYSTEM_DIR}/hyper/mydb.hyper"
    process_parameters = {"default_database_version": "2"}
    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, parameters=process_parameters) as hyper:
        with Connection(hyper.endpoint, db_path, CreateMode.CREATE_IF_NOT_EXISTS) as con:
            run_experiments('hyper', HYPER_FUNCTIONS, con)


if __name__ == '__main__':
    main()
