import os
import sys
import subprocess


SYSTEM_DIR = os.path.dirname(__file__)
sys.path.append(f'{SYSTEM_DIR}/..')
from util.util import *


def main():
    meta_server = subprocess.Popen(f'{SYSTEM_DIR}/databend/bin/databend-meta --single'.split(' '))
    query_server = subprocess.Popen(f'{SYSTEM_DIR}/databend/bin/databend-meta --single'.split(' '))
    time.sleep(10)
    try:
        client = clickhouse_connect.get_client()
        for sf in SCALE_FACTORS:
            client.query(get_schema(sf, True))
            if client.query(f"""SELECT count(*) FROM lineitem{sf}""").result_rows[0][0] == 0:
                print(f'Loading clickhouse SF{sf} ...')
                insert_file(client, f'lineitem{sf}', get_csv_path(sf))
                print(f'Loading clickhouse SF{sf} done.')
    except Exception as e:
        my_exception = e
    finally:
        query_server.terminate()
        meta_server.terminate()
    raise my_exception


if __name__ == '__main__':
    main()
