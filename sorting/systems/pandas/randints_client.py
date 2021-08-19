import pandas as pd
import os
import subprocess
import time
from tqdm import tqdm

queries = [
    '10',
    '100',
    '100_asc',
    '100_desc',
    '20',
    '30',
    '40',
    '50',
    '60',
    '70',
    '80',
    '90'
]

def run(data_folder, query_folder, results_folder):
    for qname in tqdm(queries):
        csv_name = qname.replace('_', '') + '.csv'
        qname += '.sql'
        df = pd.read_csv(data_folder + csv_name, names=['i'])

        # skip if already done
        if (os.path.isfile(results_folder + qname)):
            continue

        # time and execute the query
        for i in range(5):
            before = time.time()
            output = df.sort_values(by=['i'], inplace=False)
            after = time.time()

            del output

            # write time to csv
            with open(results_folder + 'results.csv', 'a+') as f:
                print(qname.split('.')[0] + f',{after - before}', file=f)

        # create empty file to mark query as done
        open(results_folder + qname , 'w+')

def main():
    run('../../data/randints/data/', '../../queries/randints/pandas/', '../../results/pandas/randints/')

if __name__ == '__main__':
    main()
