import os
import subprocess
import time
from tqdm import tqdm

def run(con, query_folder, results_folder):
    qnames = [q for q in os.listdir(query_folder) if q.endswith('.sql')]
    qnames = sorted(qnames, key=lambda s: [int(t) if t.isdigit() else t.lower() for t in re.split('\d+()', s)])
    for qname in tqdm(qnames):
        # skip if already done
        if (os.path.isfile(results_folder + qname)):
            continue

        # read the query
        with open(query_folder + qname, 'r') as f:
            query = f.read()

        # time and execute the query
        for i in range(5):
            subprocess.run(f'echo "DROP TABLE IF EXISTS output;" {con}', shell=True, capture_output=True)
            
            before = time.time()
            subprocess.run(f'echo "{query}" {con}', shell=True, capture_output=True)
            after = time.time()

            # write time to csv
            with open(results_folder + 'results.csv', 'a+') as f:
                print(qname.split('.')[0] + f',{after - before}', file=f)

        # create empty file to mark query as done
        open(results_folder + qname, 'w+')

def main():
    con = '| psql -U raasveld -p 7484 -h localhost mydb'
    run(con, '../../queries/randints/sql/', '../../results/hyper/randints/')

if __name__ == '__main__':
    main()
