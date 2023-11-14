import os
import sys
import subprocess
from databend_py import Client


SYSTEM_DIR = os.path.dirname(__file__)
sys.path.append(f'{SYSTEM_DIR}/..')
from util.util import *


def main():
    meta_server = subprocess.Popen(f'{SYSTEM_DIR}/databend/bin/databend-meta --single'.split(' '))
    query_server = subprocess.Popen(f'{SYSTEM_DIR}/databend/bin/databend-meta --single'.split(' '))
    time.sleep(10)
    try:
        client = Client(database='default', host='localhost', port=8124, user='root', password='root', secure=False)
        for sf in SCALE_FACTORS:
            client.execute(f"""{get_schema(sf)}""")
            _, results = client.execute(f"""SELECT count(*) FROM lineitem{sf}""")
            if results[0][0] == 0:
                print(f'Loading databend SF{sf} ...')
                client.execute(f"""COPY INTO lineitem{sf} FROM '{get_csv_path(sf)}' format csv csv_header = 1 csv_delimitor = ','""")
                print(f'Loading databend SF{sf} done.')
    except Exception as e:
        my_exception = e
    finally:
        query_server.terminate()
        meta_server.terminate()
    raise my_exception


if __name__ == '__main__':
    main()
