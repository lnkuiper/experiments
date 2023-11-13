import os
import sys
import clickhouse_connect


SYSTEM_DIR = os.path.dirname(__file__)
sys.path.append(f'{SYSTEM_DIR}/..')
from util.util import *


def run_query(query, client):
    return client.query(query).result_rows


def main():
    client = clickhouse_connect.get_client()
    run_benchmark('clickhouse', run_query, client)


if __name__ == '__main__':
    main()
