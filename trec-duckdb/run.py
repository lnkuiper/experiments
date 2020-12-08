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

t = time_millis()
con.execute("PRAGMA create_fts_index('documents', 'docno', 'documents', 'text', 'action', 'agency', 'summary', 'supplem', 'usbureau', 'usdept', 'signjob', 'footnote', 'table', 'doctitle', 'ti', 'ul', 'h2', 'address', 'headline', 'byline', 'co', 'cn', 'in', 'pe', 'ht', 'h3', 'f', 'h5', 'h4', 'h6', 'fig', 'phrase', 'p', 'graphic', 'subject', 'tablecell', 'correction');")
print("index", (time_millis() - t))

con.execute('CREATE TABLE results (query INT, q0 VARCHAR, rank INT, score DOUBLE, standard VARCHAR);')
for query in topic_dict:
    t = time_millis()
    con.execute("INSERT INTO results SELECT '" + query + "' AS topic, 'Q0' AS q0, docno, row_number() OVER (PARTITION BY (SELECT NULL)) AS rank, 0 AS score, 'STANDARD' as standard FROM documents WHERE fts_main_documents.match_bm25(docno, '" + topic_dict[query] + "');")
    print(query, (time_millis() - t))

