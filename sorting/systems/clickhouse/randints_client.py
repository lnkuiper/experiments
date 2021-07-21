from clickhouse_driver import Client
import os
import subprocess
import time
from tqdm import tqdm

def run(con, query_folder, results_folder):
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
            con.execute('DROP TABLE IF EXISTS output;')

            before = time.time()
            con.execute(query)
            after = time.time()

            # write time to csv
            with open(results_folder + 'results.csv', 'a+') as f:
                print(qname.split('.')[0] + f',{after - before}', file=f)

        # create empty file to mark query as done
        open(results_folder + qname, 'w+')

def main():
    con = Client(host = 'localhost', port = '9001')
    con.execute('set max_memory_usage=100000000000;')
    con.execute('set max_bytes_before_external_sort=80000000000;')
    run(con, '../../queries/randints/clickhouse/', '../../results/clickhouse/randints/')

if __name__ == '__main__':
    main()
