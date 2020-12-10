import duckdb
import os
import multiprocessing
import pandas as pd
import time
from tqdm import tqdm
from bs4 import BeautifulSoup as bs

def time_millis():
    return time.time()*1000.0

base_dir = '../../trec/'

t4_cr_e_dir = 'TREC_VOL_4/cr/efiles/xml/'
t4_cr_h_dir = 'TREC_VOL_4/cr/hfiles/xml/'
t4_fr94_dir = 'TREC_VOL_4/fr94/xml/'
t4_ft_dir = 'TREC_VOL_4/ft/xml/'
t5_fbis_dir = 'TREC_VOL_5/fbis/xml/'
t5_latimes_dir = 'TREC_VOL_5/latimes/xml/'
collections = [t4_cr_e_dir, t4_cr_h_dir, t4_fr94_dir, t4_ft_dir, t5_fbis_dir, t5_latimes_dir]
files = []
for dname in collections:
    for fname in os.listdir(base_dir + dname):
        files.append(base_dir + dname + fname)

def process_file(fpath):
    dict_list = []
    with open(fpath, 'r') as f:
        content = f.read()
        bs_content = bs(content, "html.parser")
        for doc in bs_content.findChildren('doc', recursive=True):
            row_dict = {}
            for c in doc.findChildren(recursive=True):
                if len(c.findChildren(recursive=True)) > 0:
                    row_dict[c.name] = c.get_text().strip().replace('\n', ' ')
                    continue
                if not c.string:
                    continue
                if row_dict.get(c.name, False):
                    row_dict[c.name] += ' ' + c.string.strip().replace('\n', ' ')
                else:
                    row_dict[c.name] = c.string.strip().replace('\n', ' ')
            dict_list.append(row_dict)
    return dict_list

pool = multiprocessing.Pool(int(multiprocessing.cpu_count()/2) + 1)
list_of_dict_lists = []
for x in tqdm(pool.imap_unordered(process_file, files), total=len(files)):
    list_of_dict_lists.append(x)
pool.close()

documents_df = pd.DataFrame([x for sublist in list_of_dict_lists for x in sublist])

con = duckdb.connect(database='db/trec04_05.db', read_only=False)
con.execute('PRAGMA threads=32');
con.register('documents_df', documents_df)
con.execute('CREATE TABLE documents AS (SELECT * FROM documents_df);')

t = time_millis()
con.execute("PRAGMA create_fts_index('documents', 'docno', 'text', overwrite=1);")
print("index", (time_millis() - t))

con.close()

