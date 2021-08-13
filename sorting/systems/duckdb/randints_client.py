import duckdb
import os
import subprocess
import time
from tqdm import tqdm

def run(query_folder, results_folder):
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
            con = duckdb.connect('randints.db', read_only=True)
            con.execute("PRAGMA threads=8;")
            con.execute("PRAGMA memory_limit='80GB'") 

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
    run('../../queries/randints/sql/', '../../results/duckdb/randints/')
    run('../../queries/randints/duckdb/', '../../results/duckdb/randints_threads/')

if __name__ == '__main__':
    main()
