import os
import shutil
import subprocess
import xml.etree.ElementTree as ET


XPATH = '/trace-toc/run[@number="1"]/data/table[@schema="counters-profile"]'
EXPORT_CMD = f'xctrace export --input sim.trace --xpath \'{XPATH}\' > trace.xml'
RECORD_CMD = 'xcrun xctrace record --template CacheMiss.tracetemplate --output "sim.trace" --launch simulation'


def create_csv_header(csv):
    print("sim,category,count,columns,L2_TLB_MISS_DATA,L1D_CACHE_MISS_LD,L1D_CACHE_MISS_ST,L1D_TLB_MISS", file=csv)
    csv.flush()


def append_row_to_file(csv, sim, category, count, columns, counters):
    print(f"{sim},{category},{count},{columns},{counters[0]},{counters[1]},{counters[2]},{counters[3]}", file=csv)
    csv.flush()


def trace(args):
    if os.path.exists('sim.trace'):
        shutil.rmtree('sim.trace')
    subprocess.run(RECORD_CMD + args, shell=True, capture_output=True)


def get_counters():
    subprocess.run(EXPORT_CMD, shell=True, capture_output=True)
    tree = ET.parse('trace.xml')
    root = tree.getroot()
    L2_TLB_MISS_DATA = 0
    L1D_CACHE_MISS_LD = 0
    L1D_CACHE_MISS_ST = 0
    L1D_TLB_MISS = 0
    for event in root[0].findall('.//row/pmc-events'):
        if not event.text:
            continue
        counts = [int(c) for c in event.text.split(' ')]
        L2_TLB_MISS_DATA += counts[0]
        L1D_CACHE_MISS_LD += counts[1]
        L1D_CACHE_MISS_ST += counts[2]
        L1D_TLB_MISS += counts[3]
    shutil.rmtree('sim.trace')
    os.remove('trace.xml')
    return (L2_TLB_MISS_DATA, L1D_CACHE_MISS_LD, L1D_CACHE_MISS_ST, L1D_TLB_MISS)


def main():
    with open('results/trace.csv', 'w+') as f:
        create_csv_header(f)
        for sim in ['reorder', 'comparator', 'sort', 'merge']:
            for category in ['col', 'row']:
                for count in range(10, 26):
                    for columns in range(1, 9):
                        args = f' {sim} {category} {count} {columns}'
                        for rep in range(5):
                            trace(args)
                            counters = get_counters()
                            append_row_to_file(f, sim, category, count, columns, counters)


if __name__ == '__main__':
    main()
