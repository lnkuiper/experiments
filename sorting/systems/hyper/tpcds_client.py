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
            subprocess.run(f'echo "DROP TABLE IF EXISTS output;" {con}', shell=True)

            before = time.time()
            subprocess.run(f'echo "{query}" {con}', shell=True)
            after = time.time()

            # write time to csv
            with open(results_folder + 'results.csv', 'a+') as f:
                print(qname.split('.')[0] + f',{after - before}', file=f)

        # create empty file to mark query as done
        open(results_folder + qname, 'w+')

def main():
    sf = os.environ['SF']
    con = '| sudo docker exec -i hyper-container psql -U raasveld -p 7484 -h localhost test'
    run(con, '../../queries/tpcds/catalog_sales/sql/', f'../../results/hyper/tpcds/sf{sf}/catalog_sales/')
    run(con, '../../queries/tpcds/customer/sql/', f'../../results/hyper/tpcds/sf{sf}/customer/')

if __name__ == '__main__':
    main()