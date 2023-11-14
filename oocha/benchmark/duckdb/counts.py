import os
import sys


SYSTEM_DIR = os.path.dirname(__file__)
sys.path.append(f'{SYSTEM_DIR}/..')
from util.util import *


def run_query(query, con):
    return con.execute(query).fetchall()


def main():
    data_con = duckdb.connect(f'{SYSTEM_DIR}/data.db', read_only=True)

    # get the 'thin' queries
    queries = get_queries(thin_only=True)
    
    counts_con = duckdb.connect(f'{QUERIES_DIR}/counts.db')
    counts_con.execute("""CREATE TABLE IF NOT EXISTS counts (grouping VARCHAR, sf USMALLINT, c UBIGINT);""")
    for sf in SCALE_FACTORS:
        print(f'Counting SF{sf} ...')
        for grouping, _, query in tqdm.tqdm(queries):
            if counts_con.execute(f"""SELECT count(*) FROM counts WHERE grouping = '{grouping}' AND sf = {sf};""").fetchall()[0][0] == 0:
                count = data_con.execute(f"""SELECT count(*) FROM ({query.replace('lineitem', f'lineitem{sf}').replace('OFFSET offset', '')}) sq;""").fetchall()[0][0]
                counts_con.execute(f"""INSERT INTO counts VALUES ('{grouping}', {sf}, {count});""")
        print(f'Counting SF{sf} done.')


if __name__ == '__main__':
    main()
