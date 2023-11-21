import os
import sys
import clickhouse_connect
from clickhouse_connect.driver.tools import insert_file
import subprocess


SYSTEM_DIR = os.path.dirname(__file__)
sys.path.append(f'{SYSTEM_DIR}/..')
from util.util import *


def main():
    os.chdir('/data/clickhouse')
    server = subprocess.Popen(f'{SYSTEM_DIR}/clickhouse/clickhouse server'.split(' '), stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    time.sleep(10)
    try:
        client = clickhouse_connect.get_client()
        for sf in SCALE_FACTORS:
            client.query(get_schema(sf, 'clickhouse'))
            if client.query(f"""SELECT count(*) FROM lineitem{sf}""").result_rows[0][0] == 0:
                print(f'Loading clickhouse SF{sf} ...')
                insert_file(client, f'lineitem{sf}', get_csv_path(sf))
                print(f'Loading clickhouse SF{sf} done.')
    except Exception as e:
        my_exception = e
    finally:
        server.terminate()
    raise my_exception


if __name__ == '__main__':
    main()
