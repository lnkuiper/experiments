import sqlite3
import os
import subprocess
import time
from tqdm import tqdm

def run(sf, query_folder, results_folder):
    for qname in tqdm(os.listdir(query_folder)):
        # skip .keep file
        if not qname.endswith('.sql'):
            continue

        # skip if already done
        if (os.path.isfile(results_folder + qname)):
            continue

        # read the query
        with open(query_folder + qname, 'r') as f:
            query = f.read()

        # time and execute the query
        for i in range(5):
            con = sqlite3.connect(f'tpcds_sf{sf}.db')

            before = time.time()
            con.execute(query)
            after = time.time()

            con.close()

            # write time to csv
            with open(results_folder + 'results.csv', 'a+') as f:
                print(qname.split('.')[0] + f',{after - before}', file=f)

        # create empty file to mark query as done
        open(results_folder + qname, 'w+')

def main():
    sf = os.environ['SF']
    run(sf, '../../queries/tpcds/catalog_sales/sql/', f'../../results/sqlite3/tpcds/sf{sf}/catalog_sales/')
    run(sf, '../../queries/tpcds/customer/sql/', f'../../results/sqlite3/tpcds/sf{sf}/customer/')

if __name__ == '__main__':
    main()