from clickhouse_driver import Client
import os
import subprocess
import time

def run(con, query_folder, results_folder):
    for qname in os.listdir(query_folder):
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
            before = time.clock()
            con.execute(query)
            after = time.clock()

            # write time to csv
            with open(results_folder + 'results.csv', 'a+'):
                print(qname.split('.')[0] + f', {after - before}')

            con.execute('DROP TABLE output;')

        # create empty file to mark query as done
        open(results_folder + qname, 'w+')

def main():
    subprocess.run('./stop.sh', shell=True)
    subprocess.run('./start.sh', shell=True)
    subprocess.run('./load_randints.sh', shell=True)
    con = Client(host = 'clickhouse-server', port = '9000', database='db_name')
    run(con, '../../queries/randints/clickhouse/', '../../results/clickhouse/randints/')

if __name__ == '__main__':
    main()
