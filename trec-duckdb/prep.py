import duckdb
import os
import pandas as pd
from tqdm import tqdm
from bs4 import BeautifulSoup as bs

t4_cr_e_dir = 'TREC_VOL_4/cr/efiles/xml/'
t4_cr_h_dir = 'TREC_VOL_4/cr/hfiles/xml/'
t4_fr94_dir = 'TREC_VOL_4/fr94/xml/'
t4_ft_dir = 'TREC_VOL_4/ft/xml/'
t5_fbis_dir = 'TREC_VOL_5/fbis/xml/'
t5_latimes_dir = 'TREC_VOL_5/latimes/xml/'
collections = [t4_cr_e_dir, t4_cr_h_dir, t4_fr94_dir, t4_ft_dir, t5_fbis_dir, t5_latimes_dir]

def process_dir(df, dir_name):
    for fname in tqdm(os.listdir(dir_name)):
        with open(dir_name + fname, 'r') as f:
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
                df = df.append(row_dict, ignore_index=True)
    return df

documents_df = pd.DataFrame()
for col in collections:
    documents_df = process_dir(documents_df, col)

con = duckdb.connect(database='db/trec04_05.db', read_only=False)
con.register('documents', documents_df)
con.close()

