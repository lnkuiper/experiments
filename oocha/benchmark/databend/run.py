import os
import sys
import subprocess
from databend_py import Client


SYSTEM_DIR = os.path.dirname(__file__)
sys.path.append(f'{SYSTEM_DIR}/..')
from util.util import *


def run_query(query, client):
    _, results = client.execute(query)
    return results


def main():
    meta_server = subprocess.Popen(f'{SYSTEM_DIR}/databend/bin/databend-meta --single'.split(' '))
    query_server = subprocess.Popen(f'{SYSTEM_DIR}/databend/bin/databend-meta --single'.split(' '))
    time.sleep(10)
    try:
        client = Client(database='default', host='localhost', port=8124, user='root', password='root', secure=False)
        run_benchmark('databend', run_query, client)
    except Exception as e:
        my_exception = e
    finally:
        query_server.terminate()
        meta_server.terminate()
    raise my_exception


if __name__ == '__main__':
    main()
