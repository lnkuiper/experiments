import pandas as pd
import os
import subprocess
import time
from tqdm import tqdm

def run(df, sf, query_folder, results_folder):
    qnames = [q for q in os.listdir(query_folder) if q.endswith('.sql')]
    qnames = sorted(qnames, key=lambda s: (s[0], len(s), s))
    for qname in tqdm(os.listdir(qnames)):
        # skip if already done
        if (os.path.isfile(results_folder + qname)):
            continue

        # read the query
        with open(query_folder + qname, 'r') as f:
            query = f.readlines()
            select_cols = [c.strip() for c in query[0].split(',')]
            order_cols = [c.strip() for c in query[1].split(',')]

        # pandas cannot order by cols not in the select
        for oc in order_cols:
            if oc not in select_cols:
                select_cols.append(oc)

        # time and execute the query
        for i in range(5):
            before = time.time()
            output = df[select_cols].sort_values(by=order_cols, inplace=False)
            after = time.time()

            del output

            # write time to csv
            with open(results_folder + 'results.csv', 'a+') as f:
                print(qname.split('.')[0] + f',{after - before}', file=f)

        # create empty file to mark query as done
        open(results_folder + qname, 'w+')

def main():
    sf = os.environ['SF']
    data_folder = f'../../data/tpcds/sf{sf}/data/'

    if int(sf) != 300:
        with open('../../data/tpcds/schema/pandas_catalog_sales_schema.sql', 'r') as f:
            col_names = [c.strip() for c in f.readlines()[0].split(',')]
        df = pd.read_csv(data_folder + '1_catalog_sales.csv', names=col_names)
        run(df, sf, '../../queries/tpcds/catalog_sales/pandas/', f'../../results/pandas/tpcds/sf{sf}/catalog_sales/')

    with open('../../data/tpcds/schema/pandas_customer_schema.sql', 'r') as f:
        col_names = [c.strip() for c in f.readlines()[0].split(',')]
    df = pd.read_csv(data_folder + '22_customer.csv', names=col_names)
    run(df, sf, '../../queries/tpcds/customer/pandas/', f'../../results/pandas/tpcds/sf{sf}/customer/')

if __name__ == '__main__':
    main()
