import os
import shutil
import subprocess
import xml.etree.ElementTree as ET


XPATH = '/trace-toc/run[@number="1"]/data/table[@schema="counters-profile"]'
EXPORT_CMD = f'xctrace export --input sim.trace --xpath \'{XPATH}\' > trace.xml'
RECORD_CMD = 'xcrun xctrace record --template counters.tracetemplate --output "sim.trace" --launch simulation'


def create_csv_header(csv):
    print("category,count,columns,time,L2_TLB_MISS_DATA,L1D_CACHE_MISS_LD,L1D_CACHE_MISS_ST,L1D_TLB_MISS,BRANCH_COND_MISPRED_NONSPEC", file=csv)
    csv.flush()


def trace(args):
    if os.path.exists('sim.trace'):
        shutil.rmtree('sim.trace')
    subprocess.run(RECORD_CMD + args, shell=True, capture_output=True)


def append_run(category, count, columns, csv):
    subprocess.run(EXPORT_CMD, shell=True, capture_output=True)
    tree = ET.parse('trace.xml')
    root = tree.getroot()
    for row in root[0].findall('.//row'):
        pmc = row.find('pmc-events')
        if not pmc.text:
            continue
        st = int(row.find('sample-time').text)
        print(f"{category},{count},{columns},{row.find('sample-time').text},{pmc.text.replace(' ', ',')}", file=csv)
    csv.flush()
    shutil.rmtree('sim.trace')
    os.remove('trace.xml')


def main():
    rows = 1 << 22
    key_cols = 3
    payload_cols = 1 << 6
    configurations = [
        ('comparator', rows, key_cols, ['col_all', 'col_ss', 'col_branchless', 'row_all', 'row_iter', 'row_norm']),
        ('sort', rows, key_cols, ['pdq_static', 'radix']),
        ('merge_key', rows, key_cols, ['row', 'col']),
        ('reorder', rows, payload_cols, ['row', 'col']),
        ('merge_payload', rows, payload_cols, ['row', 'col'])
    ]
    for sim, count, columns, categories in configurations:
        fname = f'results/trace_{sim}.csv'
        with open(fname, 'w+') as f:
            create_csv_header(f)
            for category in categories:
                args = f' {sim} {category} {count} {columns}'
                print(args)
                trace(args)
                append_run(category, count, columns, f)


if __name__ == '__main__':
    main()
