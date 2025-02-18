#!python3
import os
import shutil
import subprocess
import xml.etree.ElementTree as ET


XPATH = '/trace-toc/run[@number="1"]/data/table[@schema="counters-profile"]'
EXPORT_CMD = f'xctrace export --input sim.trace --xpath \'{XPATH}\' > trace.xml'
RECORD_CMD = 'xcrun xctrace record --template counters.tracetemplate --output "sim.trace" --launch simulation'


def create_csv_header(csv):
    print("category,count,columns,distribution,time,L2_TLB_MISS_DATA,L1D_CACHE_MISS_LD,L1D_CACHE_MISS_ST,L1D_TLB_MISS,BRANCH_COND_MISPRED_NONSPEC,BRANCH_MISPRED_NONSPEC", file=csv)
    csv.flush()


def trace(args):
    if os.path.exists('sim.trace'):
        shutil.rmtree('sim.trace')
    subprocess.run(RECORD_CMD + args, shell=True, capture_output=True)


def append_run(category, count, columns, distribution, csv):
    subprocess.run(EXPORT_CMD, shell=True, capture_output=True)
    tree = ET.parse('trace.xml')
    root = tree.getroot()
    for row in root[0].findall('.//row'):
        pmc = row.find('pmc-events')
        if not pmc.text:
            continue
        st = int(row.find('sample-time').text)
        print(f"{category},{count},{columns},{distribution},{row.find('sample-time').text},{pmc.text.replace(' ', ',')}", file=csv)
    csv.flush()
    shutil.rmtree('sim.trace')
    os.remove('trace.xml')


def main():
    rows = 1 << 24
    key_cols = 3
    payload_cols = 1 << 5
    configurations = [
        # ('comparator', rows, key_cols, [
        #  'col_all', 'col_ss', 'col_branchless', 'row_all', 'row_all_branchless', 'row_iter', 'row_norm']),
        ('sort', rows, key_cols, ['pdq_static', 'radix']),
        # ('merge_key', rows, key_cols, ['row_all', 'row_all_branchless', 'row_norm', 'col_branch', 'col_branchless']),
        # ('reorder', rows, payload_cols, ['row', 'col']),
        # ('merge_payload', rows, payload_cols, ['row', 'col'])
    ]
    distributions = ['powerlaw']  # 'random', 'uniqueN',
    for sim, count, columns, categories in configurations:
        for dist in distributions:
            fname = f'results/xcode_output/trace_{sim}_{dist}.csv'
            with open(fname, 'w+') as f:
                create_csv_header(f)
                for category in categories:
                    args = f' {sim} {category} {count} {columns} {dist}'
                    print(args)
                    trace(args)
                    append_run(category, count, columns, dist, f)


if __name__ == '__main__':
    main()
