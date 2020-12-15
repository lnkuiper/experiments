import re
import duckdb
import time
from bs4 import BeautifulSoup as bs

#base_dir = '../../trec/'
base_dir = '/home/laurens/Documents/trec/'

def time_millis():
    return time.time()*1000.0

def after_tag(s, tag):
    m = re.findall(r'<' + tag + r'>([\s\S]*?)<.*>', s)
    return m[0].replace('\n', '').strip()

topic_dict = {}
with open(base_dir + 'topics', 'r') as f:
    bs_content = bs(f.read(), "lxml")
    for top in bs_content.findChildren('top'):
        top_content = top.getText()
        num = after_tag(str(top), 'num').split(' ')[1]
        title = after_tag(str(top), 'title')
        topic_dict[num] = title

t = time_millis()
con = duckdb.connect(database='db/trec04_05.db', read_only=True)
print("load", (time_millis() - t))

con.execute('PRAGMA threads=32')
con.execute("PREPARE fts_query AS (WITH scored_docs AS (SELECT *, fts_main_documents.match_bm25(docno, ?) AS score FROM documents) SELECT docno, score FROM scored_docs WHERE score IS NOT NULL ORDER BY score DESC LIMIT 1000)")

results = []
for query in topic_dict:
    q_str = topic_dict[query].replace('\'', ' ')
    t = time_millis()
    con.execute("EXECUTE fts_query('" + q_str + "');")
    print(query, (time_millis() - t))
    for i, row in enumerate(con.fetchall()):
        results.append(query + " Q0 " + row[0] + " " + str(i) + " " + str(row[1]) + " STANDARD")
con.close()

with open('results', 'w+') as f:
    for r in results:
        f.write(r + '\n')

