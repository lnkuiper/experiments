import os
import sys
import clickhouse_connect
from clickhouse_connect.driver.tools import insert_file


SYSTEM_DIR = os.path.dirname(__file__)
sys.path.append(f'{SYSTEM_DIR}/..')
from util.util import *


def main():
    client = clickhouse_connect.get_client()
    for sf in SCALE_FACTORS:
        client.query(get_schema(sf, True))
        if client.query(f"""SELECT count(*) FROM lineitem{sf}""").result_rows[0][0] == 0:
            print(f'Loading clickhouse SF{sf} ...')
            insert_file(client, f'lineitem{sf}', get_csv_path(sf))
            print(f'Loading clickhouse SF{sf} done.')


if __name__ == '__main__':
    main()
