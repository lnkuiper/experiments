import os
import sys
import shutil


SCRIPTS_DIR = os.path.dirname(__file__)
sys.path.append(f'{SCRIPTS_DIR}/..')
from benchmark.util.util import *


TABLES = {
    'lineitem': [
        'l_orderkey',
        'l_partkey',
        'l_suppkey',
        'l_linenumber',
        'l_quantity',
        'l_extendedpric',
        'l_discount',
        'l_tax',
        'l_returnflag',
        'l_linestatus',
        'l_shipdate',
        'l_commitdate',
        'l_receiptdate',
        'l_shipinstruct',
        'l_shipmode',
        'l_comment',
    ],
    'orders': [
        'o_orderkey',
        'o_custkey',
        'o_orderstatus',
        'o_totalprice',
        'o_orderdate',
        'o_orderpriority',
        'o_clerk',
        'o_shippriority',
        'o_comment',
    ],
    'partsupp': [
        'ps_partkey',
        'ps_suppkey',
        'ps_availqty',
        'ps_supplycost',
        'ps_comment',
    ],
    'part': [
        'ps_partkey',
        'ps_suppkey',
        'ps_availqty',
        'ps_supplycost',
        'ps_comment',
    ],
    'supplier': [
        's_suppkey',
        's_name',
        's_address',
        's_nationkey',
        's_phone',
        's_acctbal',
        's_comment',
    ]
}


QUERY_DEFINITIONS = [{
    'description': 'LARGE JOIN',
    'tables': [{
        'name': 'lineitem',
        'alias': 'l',
        'columns': [
            'l_orderkey',
        ]
    }, {
        'name': 'orders',
            'alias': 'o',
            'columns': [
                'o_custkey',
                'o_orderstatus',
                'o_totalprice',
                'o_orderdate',
                'o_shippriority'
            ]
    }],
    'conditions': [
        'l.l_orderkey = o.o_orderkey'
    ]
}, {
    'description': 'DOUBLE MEDIUM JOIN',
    'tables': [{
        'name': 'lineitem',
        'alias': 'l',
        'columns': [
            'l_orderkey'
        ]
    }, {
        'name': 'orders',
        'alias': 'o1',
        'columns': [
            'o_custkey',
            'o_orderstatus',
            'o_totalprice',
        ]
    }, {
        'name': 'orders',
        'alias': 'o2',
        'columns': [
            'o_orderdate',
            'o_shippriority'
        ]
    }],
    'conditions': [
        'l.l_orderkey = o1.o_orderkey',
        'o1.o_orderkey = o2.o_orderkey',
    ]
}, {
    'description': 'LARGE JOIN AND SMALL JOIN',
    'tables': [{
        'name': 'lineitem',
        'alias': 'l',
        'columns': [
            'l_orderkey',
            'l_partkey',
            'l_suppkey',
        ]
    }, {
        'name': 'orders',
        'alias': 'o',
        'columns': [
            'o_custkey',
            'o_orderstatus',
            'o_totalprice',
            'o_orderdate',
            'o_shippriority'
        ]
    }, {
        'name': 'partsupp',
        'alias': 'ps',
        'columns_except': [
            'ps_partkey',
            'ps_suppkey',
            'ps_comment',
        ]
    }],
    'conditions': [
        'l.l_orderkey = o.o_orderkey',
        'l.l_partkey = ps.ps_partkey',
        'l.l_suppkey = ps.ps_suppkey',
    ]
}, {
    'description': 'DOUBLE MEDIUM JOIN AND SMALL JOIN',
    'tables': [{
        'name': 'lineitem',
        'alias': 'l',
        'columns': [
            'l_orderkey',
            'l_partkey',
            'l_suppkey',
        ]
    }, {
        'name': 'orders',
        'alias': 'o1',
        'columns': [
            'o_custkey',
            'o_orderstatus',
            'o_totalprice',
        ]
    }, {
        'name': 'orders',
        'alias': 'o2',
        'columns': [
            'o_orderdate',
            'o_shippriority'
        ]
    }, {
        'name': 'partsupp',
        'alias': 'ps',
        'columns_except': [
            'ps_partkey',
            'ps_suppkey',
            'ps_comment',
        ]
    }],
    'conditions': [
        'l.l_orderkey = o1.o_orderkey',
        'o1.o_orderkey = o2.o_orderkey',
        'l.l_partkey = ps.ps_partkey',
        'l.l_suppkey = ps.ps_suppkey',
    ]
}, {
    'description': 'DOUBLE MEDIUM AND DOUBLE SMALL JOIN',
    'tables': [{
        'name': 'lineitem',
        'alias': 'l',
        'columns': [
            'l_orderkey',
            'l_partkey',
            'l_suppkey',
        ]
    }, {
        'name': 'orders',
        'alias': 'o1',
        'columns': [
            'o_custkey',
            'o_orderstatus',
            'o_totalprice',
        ]
    }, {
        'name': 'orders',
        'alias': 'o2',
        'columns': [
            'o_orderdate',
            'o_shippriority'
        ]
    }, {
        'name': 'partsupp',
        'alias': 'ps1',
        'columns': [
            'ps_availqty',
        ]
    }, {
        'name': 'partsupp',
        'alias': 'ps2',
        'columns': [
            'ps_supplycost',
        ]
    }],
    'conditions': [
        'l.l_orderkey = o1.o_orderkey',
        'o1.o_orderkey = o2.o_orderkey',
        'l.l_partkey = ps1.ps_partkey',
        'l.l_suppkey = ps1.ps_suppkey',
        'ps1.ps_partkey = ps2.ps_partkey',
        'ps1.ps_suppkey = ps2.ps_suppkey',
    ]
}]


def generate_query(query_definition):
    select_list = []
    from_list = []
    where_list = query_definition.get('conditions')

    table_definitions = query_definition.get('tables')
    for table_definition in table_definitions:
        table_name = table_definition.get('name')
        alias = table_definition.get('alias')
        from_list.append(f"{table_name} AS {alias}")
        if table_definition.get('columns'):
            for column in table_definition.get('columns'):
                select_list.append(f"{alias}.{column}")
        else:
            for column in TABLES[table_name]:
                if column not in table_definition.get('columns_except'):
                    select_list.append(f"{alias}.{column}")

    return f"""-- {query_definition.get('description')}
SELECT
{',\n'.join(['    ' + s for s in select_list])}
FROM
{',\n'.join(['    ' + f for f in from_list])}
WHERE
    {'\nAND '.join([c for c in where_list])}
OFFSET
    %OFFSET%;
"""


def main():
    if not os.path.exists(QUERIES_DIR):
        os.mkdir(QUERIES_DIR)

    for i, query_definition in enumerate(QUERY_DEFINITIONS):
        with open(f'{QUERIES_DIR}/{i + 1}.sql', 'w') as f:
            f.write(generate_query(query_definition))


if __name__ == '__main__':
    main()
