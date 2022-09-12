def query(table):
    return f'SELECT count(i) FROM (SELECT i FROM {table} ORDER BY i) sq;'

def threads_query(threads, table):
    return f'PRAGMA threads={threads}; ' + query(table)

with open('sql/100_asc.sql', 'w+') as f:
    print(query('ints100_asc'), file=f)

with open('sql/100_desc.sql', 'w+') as f:
    print(query('ints100_desc'), file=f)

for i in range(1, 11):
    with open(f'sql/{i * 10}.sql', 'w+') as f:
        print(query(f'ints{i * 10}'), file=f)

for i in range(1, 33):
    with open(f'duckdb_threads/{i}.sql', 'w+') as f:
        print(threads_query(i, 'ints100'), file=f)
