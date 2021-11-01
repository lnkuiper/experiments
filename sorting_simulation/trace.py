import os
import shutil
import subprocess
import xml.etree.ElementTree as ET


XPATH = '/trace-toc/run[@number="1"]/data/table[@schema="counters-profile"]'
EXPORT_CMD = f'xctrace export --input sim.trace --xpath \'{XPATH}\' > trace.xml'
RECORD_CMD = 'xcrun xctrace record --template CacheMiss.tracetemplate --output "sim.trace" --launch simulation'


def create_csv_header(csv):
    print("category,count,columns,time,L2_TLB_MISS_DATA,L1D_CACHE_MISS_LD,L1D_CACHE_MISS_ST,L1D_TLB_MISS", file=csv)
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
    for sim in ['reorder', 'comparator', 'sort', 'merge']:
        fname = f'results/trace_{sim}.csv'
        if os.path.exists(fname):
            continue
        with open(fname, 'w+') as f:
            create_csv_header(f)
            for category in ['col', 'row']:
                for count in range(10, 25):
                    for columns in range(1, 9):
                        if sim == 'reorder' or sim == 'merge':
                            columns = min(1 << (columns - 1), 96)
                        args = f' {sim} {category} {count} {columns}'
                        for rep in range(3):
                            while True:
                                try:
                                    trace(args)
                                    append_run(category, count, columns, f)
                                    break
                                except:
                                    continue


if __name__ == '__main__':
    main()
