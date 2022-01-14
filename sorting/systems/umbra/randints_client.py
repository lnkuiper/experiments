import psycopg2
import os
import re
import subprocess
import time
from tqdm import tqdm

def run(con, query_folder, results_folder):
    qnames = [q for q in os.listdir(query_folder) if q.endswith('.sql')]
    qnames = sorted(qnames, key=lambda s: (s[0], len(s), s))
    for qname in tqdm(qnames):
        # skip .keep file
        if not qname.endswith('.sql'):
            continue

        # skip if already done
        if (os.path.isfile(results_folder + qname)):
            continue

        # read the query
        with open(query_folder + qname, 'r') as f:
            query = f.read()

        table = re.search("FROM ([^ ]*)", query).group(1)
        con.execute(f"SELECT COUNT(*) FROM {table};")
        count = con.fetchall()[0][0]
        query = f"{query} LIMIT 1 OFFSET {count - 1};"

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
    con = psycopg2.connect(host="localhost", user="postgres", password="mysecretpassword", port=5432)
    cur = con.cursor()
    cur.execute("SET debug.disableoptimizer=1;")
    run(cur, '../../queries/randints/sql/', '../../results/umbra/randints/')

if __name__ == '__main__':
    main()

