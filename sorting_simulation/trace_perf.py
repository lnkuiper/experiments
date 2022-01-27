#!python3
import os
import shutil
import subprocess
import re


EXPORT_CMD = 'perf script > perf.export'
RECORD_CMD = 'perf record -e cache-misses,branch-misses -F 250 ./simulation '


def create_csv_header(csv):
    print("category|count|columns|cmd|pid|time|counter|event", file=csv)
    csv.flush()


def trace(args):
    subprocess.run(RECORD_CMD + args, shell=True, capture_output=False)


def append_run(category, count, columns, csv):
    subprocess.run(EXPORT_CMD, shell=True, capture_output=True)

    with open('perf.export', 'r') as f:
        export = '\n'.join([f'{category}|{count}|{columns}|' + '|'.join(re.split(' +', line)[1:6]) for line in f.readlines()])

    print(export, file=csv)
    os.remove('perf.data')
    os.remove('perf.export')


def main():
    rows = 1 << 24
    key_cols = 3
    payload_cols = 1 << 5
    configurations = [
        ('comparator', rows, key_cols, ['col_all', 'col_ss', 'col_branchless', 'row_all', 'row_all_branchless', 'row_iter', 'row_norm']),
        ('sort', rows, key_cols, ['pdq_static', 'radix']),
        ('merge_key', rows, key_cols, ['row_all', 'row_all_branchless', 'row_norm', 'col_branch', 'col_branchless']),
        ('reorder', rows, payload_cols, ['row', 'col']),
        ('merge_payload', rows, payload_cols, ['row', 'col'])
    ]
    for sim, count, columns, categories in configurations:
        fname = f'results/perf_output/trace_{sim}.csv'
        with open(fname, 'w+') as f:
            create_csv_header(f)
            for category in categories:
                args = f' {sim} {category} {count} {columns}'
                trace(args)
                append_run(category, count, columns, f)


if __name__ == '__main__':
    main()
