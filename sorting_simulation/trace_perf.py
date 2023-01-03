#!python3
import os
import shutil
import subprocess
import re


# In the report, each mention of either 'branch-misses' or 'L1-dcache-load-misses' means 10k more branch/l1-dcache misses at that timestamp
INTERVAL = 20000
RECORD_CMD = f'perf record -e branch-misses,L1-dcache-load-misses -c {INTERVAL} -- ./simulation '
EXPORT_CMD = 'perf script -F time,event > perf.export'


def create_csv_header(csv):
    print("category,count,columns,distribution,time,event,counter", file=csv)
    csv.flush()


def trace(args):
    subprocess.run(RECORD_CMD + args, shell=True, capture_output=False)


def append_run(category, count, columns, distribution, csv):
    subprocess.run(EXPORT_CMD, shell=True, capture_output=True)

    prefix = f'{category},{count},{columns},{distribution}'
    with open('perf.export', 'r') as f:
        for line in f.readlines():
            split_line = line.split(':')
            print(
                f'{prefix},{split_line[0]},{split_line[1].strip()},{INTERVAL}', file=csv)

    os.remove('perf.data')
    os.remove('perf.export')


def main():
    rows = 1 << 24
    key_cols = 3
    payload_cols = 1 << 5
    configurations = [
         ('comparator', rows, key_cols, [
          'col_all', 'col_ss', 'col_branchless', 'row_all', 'row_all_branchless', 'row_iter', 'row_norm']),
        #('sort', rows, key_cols, ['pdq_static', 'radix']),
        # ('merge_key', rows, key_cols, ['row_all', 'row_all_branchless', 'row_norm', 'col_branch', 'col_branchless']),
        # ('reorder', rows, payload_cols, ['row', 'col']),
        # ('merge_payload', rows, payload_cols, ['row', 'col'])
    ]
    distributions = ['powerlaw', 'random', 'uniqueN', 'correlated0.1', 'correlated0.2', 'correlated0.3',
                     'correlated0.4', 'correlated0.5', 'correlated0.6', 'correlated0.7', 'correlated0.8', 'correlated0.9']
    for sim, count, columns, categories in configurations:
        for dist in distributions:
            fname = f'results/perf_output/trace_{sim}_{dist}.csv'
            with open(fname, 'w+') as f:
                create_csv_header(f)
                for category in categories:
                    args = f' {sim} {category} {count} {columns} {dist}'
                    trace(args)
                    append_run(category, count, columns, dist, f)


if __name__ == '__main__':
    main()
