import pymonetdb
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
            try:
                con.execute('DROP TABLE output;')
            except:
                None

            subprocess.run('sudo docker exec -i monetdb-container monetdb stop test', shell=True)
            time.sleep(3)
            subprocess.run('sudo docker exec -i --user root monetdb-container /clear_cache.sh', shell=True)
            time.sleep(3)
            subprocess.run('sudo docker exec -i monetdb-container monetdb start test', shell=True)
            time.sleep(3)

            connection = pymonetdb.connect(username="monetdb", password="monetdb", hostname="localhost", port=50001, database="test")
            con = connection.cursor()

            before = time.time()
            con.execute(query)
            after = time.time()

            # write time to csv
            with open(results_folder + 'results.csv', 'a+') as f:
                print(qname.split('.')[0] + f',{after - before}', file=f)

            con.close()

        # create empty file to mark query as done
        open(results_folder + qname, 'w+')

def main():
    run('../../queries/randints/sql/', '../../results/monetdb/randints/')

if __name__ == '__main__':
    main()
