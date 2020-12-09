import re
import duckdb
import time
from bs4 import BeautifulSoup as bs

def time_millis():
    return time.time()*1000.0

def after_tag(s, tag):
    m = re.findall(r'<' + tag + r'>([\s\S]*?)<.*>', s)
    return m[0].replace('\n', '').strip()

topic_dict = {}
with open('../../trec/topics', 'r') as f:
    bs_content = bs(f.read(), "lxml")
    for top in bs_content.findChildren('top'):
        top_content = top.getText()
        num = after_tag(str(top), 'num').split(' ')[1]
        title = after_tag(str(top), 'title')
        topic_dict[num] = title

t = time_millis()
con = duckdb.connect(database='db/trec04_05.db', read_only=False)
print("load", (time_millis() - t))

con.execute('PRAGMA threads=32')

con.execute('DROP TABLE IF EXISTS results;')
con.execute('CREATE TABLE results (topic VARCHAR, docno VARCHAR, rank INT);')
for query in topic_dict:
    t = time_millis()
    con.execute("INSERT INTO results SELECT '" + query + "' AS topic, docno, row_number() OVER (PARTITION BY (SELECT NULL)) AS rank FROM documents WHERE fts_main_documents.match_bm25(docno, '" + topic_dict[query] + "');")
    print(query, (time_millis() - t))

