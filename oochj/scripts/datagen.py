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


def task(id, sf, total_steps, step_start, step_end):
    temporary_db = f'temp{id}.db'
    if os.path.exists(temporary_db):
        os.remove(temporary_db)
    con = duckdb.connect(temporary_db)
    con.execute("SET threads=1;")
    con.execute("SET memory_limit='1gb';")
    for step in range(step_start, step_end):
        con.execute(f'CALL dbgen(sf={sf}, children={total_steps}, step={step});')
    con.close()

def generate_sf(sf):
    cpu_count = multiprocessing.cpu_count()
    total_steps = int((sf + cpu_count - 1) / cpu_count) * cpu_count
    steps_per_cpu = int(total_steps / cpu_count)

    pool = [None for _ in range(cpu_count)]
    step = 0
    for i in range(cpu_count):
        pool[i] = multiprocessing.Process(
            target=task,
            args=(i, sf, total_steps, step, step + steps_per_cpu,)
        )
        pool[i].start()
        step += steps_per_cpu

    for i in range(cpu_count):
        pool[i].join()

    temporary_db = 'temp.db'
    if os.path.exists(temporary_db):
        os.remove(temporary_db)
    con = duckdb.connect('temp.db')

    con.execute("SET preserve_insertion_order=false;")
    # con.execute("SET allocator_background_threads=true;")
    con.execute("CALL dbgen(sf=0);")

    for i in range(cpu_count):
        con.execute(f"ATTACH 'temp{i}.db' AS temp{i} (READ_ONLY);")
    for table in TABLES:
        con.execute(f"INSERT INTO {table} {' UNION ALL '.join(['SELECT * FROM temp' + str(i) + '.' + table for i in range(cpu_count)])};")
    for i in range(cpu_count):
        os.remove(f'temp{i}.db')

    output = f'{DATA_DIR}/sf{sf}'
    if os.path.exists(output):
        shutil.rmtree(output)
    con.execute(f"EXPORT DATABASE '{output}';")

    con.close()
    os.remove('temp.db')


def main():
    if not os.path.exists(DATA_DIR):
        os.mkdir(DATA_DIR)

    for sf in SCALE_FACTORS:
        print(f'Generating SF{sf} ...')
        generate_sf(sf)
        print(f'Generating SF{sf} done.')


if __name__ == '__main__':
    main()
