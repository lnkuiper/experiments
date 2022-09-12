import duckdb
import os
import re
import subprocess
import time
from tqdm import tqdm

def run(con, query_folder, results_folder):
    qnames = [q for q in os.listdir(query_folder) if q.endswith('.sql')]
    qnames = sorted(qnames, key=lambda s: (s[0], len(s), s))
    for qname in tqdm(qnames):
        # skip if already done
        if (os.path.isfile(results_folder + qname)):
            continue

        # read the query
        with open(query_folder + qname, 'r') as f:
            query = f.read()

        # time and execute the query
        for i in range(5):
            before = time.time()
            con.execute(query)
            after = time.time()

            # write time to csv
            with open(results_folder + 'results.csv', 'a+') as f:
                print(qname.split('.')[0] + f',{after - before}', file=f)

        # create empty file to mark query as done
        open(results_folder + qname, 'w+')

def main():
    sf = os.environ['SF']
    con = duckdb.connect(f'tpcds_sf{sf}.db', read_only=True)
    con.execute('PRAGMA threads=16')
    if int(sf) != 300:
        run(con, '../../queries/tpcds/catalog_sales/sql/', f'../../results/duckdb/tpcds/sf{sf}/catalog_sales/')
    run(con, '../../queries/tpcds/customer/sql/', f'../../results/duckdb/tpcds/sf{sf}/customer/')

if __name__ == '__main__':
    main()
