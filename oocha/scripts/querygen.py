import os
import sys
import shutil


SCRIPTS_DIR = os.path.dirname(__file__)
sys.path.append(f'{SCRIPTS_DIR}/..')
from benchmark.util.util import *


GROUPINGS = [
    ['l_returnflag', 'l_linestatus'], # 4 rows
    ['l_shipinstruct'], # 4 rows
    ['l_shipmode'], # 7 rows
    ['l_suppkey'], # 0.17%
    ['l_suppkey', 'l_returnflag', 'l_linestatus'], # 0.66%
    ['l_partkey'], # 3.33%
    ['l_partkey', 'l_returnflag', 'l_linestatus'], # 10.58%
    ['l_suppkey', 'l_partkey'], # 13.33%
    ['l_orderkey'], # 25%
    ['l_orderkey', 'l_returnflag', 'l_linestatus'], # 34.87%
    ['l_suppkey', 'l_partkey', 'l_returnflag', 'l_linestatus'], # 36.17%
    ['l_suppkey', 'l_partkey', 'l_shipinstruct'], # 45.34%
    ['l_suppkey', 'l_partkey', 'l_shipmode'], # 61.83%
    ['l_suppkey', 'l_partkey', 'l_shipinstruct', 'l_shipmode'], # 88.56%
    ['l_orderkey', 'l_partkey'], # 99.99%
    ['l_orderkey', 'l_suppkey'], # 99.99%
    ['l_suppkey', 'l_partkey', 'l_orderkey'], # 100.00%
]


ALL_COLUMNS = [
    'l_orderkey',
    'l_partkey',
    'l_suppkey',
    'l_linenumber',
    'l_quantity',
    'l_extendedprice',
    'l_discount',
    'l_tax',
    'l_returnflag',
    'l_linestatus',
    'l_shipdate',
    'l_commitdate',
    'l_receiptdate',
    'l_shipinstruct',
    'l_shipmode',
    'l_comment'
]


def generate_query(grouping, wide):
    selected_columns = ALL_COLUMNS if wide else grouping
    subquery = f"""SELECT DISTINCT ON ({', '.join(grouping)}) {', '.join(selected_columns)} FROM lineitem"""
    return f"""SELECT {', '.join([f'count({c})' for c in selected_columns])} FROM ({subquery}) sq;\n"""


def main():
    if not os.path.exists(QUERIES_DIR):
        os.mkdir(QUERIES_DIR)

    if not os.path.exists(THIN_QUERIES_DIR):
        os.mkdir(THIN_QUERIES_DIR)

    if not os.path.exists(WIDE_QUERIES_DIR):
        os.mkdir(WIDE_QUERIES_DIR)

    for grouping in GROUPINGS:
        file_name = f"""{'-'.join(grouping)}.sql"""
        with open(f'{THIN_QUERIES_DIR}/{file_name}', 'w') as f:
            f.write(generate_query(grouping, False))
        with open(f'{WIDE_QUERIES_DIR}/{file_name}', 'w') as f:
            f.write(generate_query(grouping, True))


if __name__ == '__main__':
    main()
