import duckdb
import os
import time


def queries():
    result = []
    for q in sorted(os.listdir('queries')):
        with open(f'queries/{q}', 'r') as f:
            result.append((q.replace('.sql', ''), f.read()))
    return result


def load_lineitem(con):
    con.execute("CREATE TABLE lineitem_json (json JSON);")
    con.execute("INSERT INTO lineitem_json SELECT * FROM read_json_objects('/Users/laurens/git/experiments/json/data/lineitem.json');")
    con.execute("""CREATE VIEW lineitem AS SELECT j.l_orderkey AS l_orderkey, j.l_partkey AS l_partkey, j.l_suppkey AS l_suppkey, j.l_linenumber AS l_linenumber, j.l_quantity AS l_quantity, j.l_extendedprice AS l_extendedprice, j.l_discount AS l_discount, j.l_tax AS l_tax, j.l_returnflag AS l_returnflag, j.l_linestatus AS l_linestatus, j.l_shipdate AS l_shipdate, j.l_commitdate AS l_commitdate, j.l_receiptdate AS l_receiptdate, j.l_shipinstruct AS l_shipinstruct, j.l_shipmode AS l_shipmode, j.l_comment AS l_comment FROM (SELECT from_json(json, '{"l_orderkey": "INTEGER", "l_partkey": "INTEGER", "l_suppkey": "INTEGER", "l_linenumber": "INTEGER", "l_quantity": "INTEGER", "l_extendedprice": "DECIMAL(15,2)", "l_discount": "DECIMAL(15,2)", "l_tax": "DECIMAL(15,2)", "l_returnflag": "VARCHAR", "l_linestatus": "VARCHAR", "l_shipdate": "DATE", "l_commitdate": "DATE", "l_receiptdate": "DATE", "l_shipinstruct": "VARCHAR", "l_shipmode": "VARCHAR", "l_comment": "VARCHAR"}') AS j FROM lineitem_json);""")
    con.execute("SELECT * FROM lineitem")
    con.fetchdf()


def main():
    # con = duckdb.connect('tpch_json.duckdb')
    # for qnum, query in queries():
    #     for i in range(5):
    #         before = time.time()
    #         con.execute(query)
    #         after = time.time()
    #         print(f'{qnum}\tduckdb\t{after - before}')
    con = duckdb.connect()
    load_lineitem(con)


if __name__ == '__main__':
    main()
