import os
import duckdb
import multiprocessing
import shutil
import sys


SCRIPTS_DIR = os.path.dirname(__file__)
sys.path.append(f'{SCRIPTS_DIR}/..')
from benchmark.util.util import *


TABLES = [
    'customer',
    'lineitem',
    'nation',
    'orders',
    'part',
    'partsupp',
    'region',
    'supplier',
]


def task(arg_dict):
    temporary_db = f"temp{arg_dict.get('id')}.db"
    if os.path.exists(temporary_db):
        os.remove(temporary_db)
    con = duckdb.connect(temporary_db)
    con.execute("SET threads=1;")
    con.execute("SET memory_limit='1gb';")
    for step in range(arg_dict.get('step_start'), arg_dict.get('step_end')):
        con.execute(f"CALL dbgen(sf={arg_dict.get('sf')}, children={arg_dict.get('total_steps')}, step={step});")
    con.close()

def generate_sf(sf):
    cpu_count = multiprocessing.cpu_count()
    total_steps = int((sf + cpu_count - 1) / cpu_count) * cpu_count
    steps_per_cpu = int(total_steps / cpu_count)

    tasks = []
    step = 0
    for i in range(cpu_count):
        tasks.append({
            'id': i,
            'sf': sf,
            'total_steps': total_steps,
            'step_start': step,
            'step_end': step + steps_per_cpu,
        })
        step += steps_per_cpu

    with multiprocessing.Pool(cpu_count) as p:
        p.map(task, tasks)

    temporary_db = 'temp.db'
    if os.path.exists(temporary_db):
        os.remove(temporary_db)
    con = duckdb.connect('temp.db')

    con.execute("SET preserve_insertion_order=false;")
    # con.execute("SET allocator_background_threads=true;")
    con.execute("CALL dbgen(sf=0);")

    for i in range(cpu_count):
        con.execute(f"ATTACH 'temp{i}.db' AS temp{i} (READ_ONLY);")
        con.execute("START TRANSACTION;")
        for table in TABLES:
            con.execute(f"INSERT INTO {table} SELECT * FROM temp{i}.{table};")
        con.execute("COMMIT;")
        con.execute(f"DETACH temp{i};")
        os.remove(f'temp{i}.db')

    output = f'{DATA_DIR}/sf{sf}'
    if os.path.exists(output):
        shutil.rmtree(output)
    con.execute(f"EXPORT DATABASE '{output}';")

    con.close()
    os.remove(temporary_db)

    with open(f"{output}/load.sql", 'r') as f:
        header_true = f.read().replace('header 1', 'header TRUE')
    with open(f"{output}/load.sql", 'w') as f:
        f.write(header_true)

def main():
    if not os.path.exists(DATA_DIR):
        os.mkdir(DATA_DIR)

    for sf in SCALE_FACTORS:
        output = f'{DATA_DIR}/sf{sf}'
        if os.path.exists(output):
            continue
        print(f'Generating SF{sf} ...')
        generate_sf(sf)
        print(f'Generating SF{sf} done.')


if __name__ == '__main__':
    main()
