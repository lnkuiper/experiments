import argparse
import os
import duckdb
import multiprocessing
import shutil
import sys


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
    con.execute("PRAGMA disable_progress_bar;")
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

    temporary_db = f'tpch-sf{sf}.db'
    if os.path.exists(temporary_db):
        os.remove(temporary_db)
    con = duckdb.connect(temporary_db)

    con.execute("CALL dbgen(sf=0);")
    for i in range(cpu_count):
        con.execute(f"ATTACH 'temp{i}.db' AS temp{i} (READ_ONLY);")
        con.execute("START TRANSACTION;")
        for table in TABLES:
            con.execute(f"INSERT INTO {table} SELECT * FROM temp{i}.{table};")
        con.execute("COMMIT;")
        con.execute(f"DETACH temp{i};")
        os.remove(f'temp{i}.db')
    con.close()

def main():
    parser = argparse.ArgumentParser(prog='dbgen.py', description='Generates TPC-H at the given SF in parallel using DuckDB', epilog='Example: python3 dbgen.py 10')
    parser.add_argument('sf')
    args = parser.parse_args()

    sf = int(args.sf)
    print(f'Generating SF{sf} ...')
    generate_sf(sf)
    print(f'Generating SF{sf} done.')


if __name__ == '__main__':
    main()
