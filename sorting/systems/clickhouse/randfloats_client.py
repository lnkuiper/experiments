from clickhouse_driver import Client
import os
import subprocess
import time
from tqdm import tqdm

def run(con, query_folder, results_folder, threads=False):
    qnames = [q for q in os.listdir(query_folder) if q.endswith('.sql')]
    qnames = sorted(qnames, key=lambda s: (s[0], len(s), s))
    for qname in tqdm(qnames):
        # skip if already done
        if (os.path.isfile(results_folder + qname)):
            continue

        if threads:
            t = int(qname.split('.sql')[0])
            con.execute(f'set max_threads={t};')

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
    con = Client(host = 'localhost', port = '9000')
    #con.execute('set max_threads=16;')
    #con.execute('set optimize_trivial_count_query=0;')
    run(con, '../../queries/randfloats/sql/', '../../results/clickhouse/randfloats/')
    #run(con, '../../queries/randints/clickhouse/threads/', '../../results/clickhouse/randints_threads/', True)

if __name__ == '__main__':
    main()
