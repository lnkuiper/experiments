import os
import sys
import clickhouse_connect
import subprocess


SYSTEM_DIR = os.path.dirname(__file__)
sys.path.append(f'{SYSTEM_DIR}/..')
from util.util import *


def run_query(query, client):
    return client.query(query).result_rows


def main():
    server = subprocess.Popen(f'{SYSTEM_DIR}/clickhouse/clickhouse server'.split(' '), stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    time.sleep(10)
    try:
        client = clickhouse_connect.get_client()
        run_benchmark('clickhouse', run_query, client)
    except Exception as e:
        my_exception = e
    finally:
        server.terminate()
    raise my_exception


if __name__ == '__main__':
    main()
