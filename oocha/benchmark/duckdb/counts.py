import os
import sys


SYSTEM_DIR = os.path.dirname(__file__)
sys.path.append(f'{SYSTEM_DIR}/..')
from util.util import *


def run_query(query, con):
    return con.execute(query).fetchall()


def main():
    con = duckdb.connect(f'{SYSTEM_DIR}/data.db', read_only=True)

    # get the 'thin' queries
    queries = get_queries()
    queries = queries[int(len(queries) / 2):]

    d = {}
    for grouping, _, _ in queries:
        d[grouping] = {}
    
    for sf in SCALE_FACTORS:
        print(f'Counting SF{sf} ...')
        for grouping, _, query in tqdm.tqdm(queries):
            q = f"""SELECT count(*) FROM ({query.replace('lineitem', f'lineitem{sf}').replace('OFFSET offset', '')}) sq"""
            d[grouping][sf] = con.execute(q).fetchall()[0][0]
        print(f'Counting SF{sf} done.')
        print(d)


if __name__ == '__main__':
    main()
