def query(table):
    return f'SELECT last_value(i) OVER () FROM (SELECT * FROM {table} ORDER BY i) sq LIMIT 1;'

def threads_query(threads, table):
    return f'PRAGMA threads={threads}; ' + query(table)

with open('sql/100_asc.sql', 'w+') as f:
    print(query('ints100_asc'), file=f)

with open('sql/100_desc.sql', 'w+') as f:
    print(query('ints100_desc'), file=f)

for i in range(1, 11):
    with open(f'sql/{i * 10}.sql', 'w+') as f:
        print(query(f'ints{i * 10}'), file=f)

for i in range(1, 17):
    with open(f'duckdb/{i}.sql', 'w+') as f:
        print(threads_query(i, 'ints100'), file=f)
