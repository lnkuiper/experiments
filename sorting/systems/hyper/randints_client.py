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
            before = time.time()
            subprocess.run(f'echo "{query}" {con}', shell=True, capture_output=True)
            after = time.time()

            # write time to csv
            with open(results_folder + 'results.csv', 'a+') as f:
                print(qname.split('.')[0] + f',{after - before}', file=f)

            subprocess.run(f'echo "DROP TABLE output;" {con}', shell=True, capture_output=True)

        # create empty file to mark query as done
        open(results_folder + qname, 'w+')

def main():
    con = '| docker exec -i hyper-container psql -U raasveld -p 7484 -h localhost test'
    run(con, '../../queries/randints/sql/', '../../results/hyper/randints/')

if __name__ == '__main__':
    main()