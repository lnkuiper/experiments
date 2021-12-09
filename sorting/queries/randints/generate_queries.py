def query(table, ch=False):
    q = 'CREATE '
    if ch:
        q += 'TABLE output ENGINE = File(Native)'
    else:
        q += 'TEMPORARY TABLE output'
    q += ' (i INT);'
    return q + f'INSERT INTO output SELECT * FROM {table} ORDER BY i;'

def threads_query(threads, table):
    return f'PRAGMA threads={threads}; ' + query(table)

with open('sql/100_asc.sql', 'w+') as f:
    print(query('ints100_asc'), file=f)
with open('clickhouse/100_asc.sql', 'w+') as f:
    print(query('ints100_asc', ch=True), file=f)

with open('sql/100_desc.sql', 'w+') as f:
    print(query('ints100_desc'), file=f)
with open('clickhouse/100_desc.sql', 'w+') as f:
    print(query('ints100_desc', ch=True), file=f)

for i in range(1, 11):
    with open(f'sql/{i * 10}.sql', 'w+') as f:
        print(query(f'ints{i * 10}'), file=f)
    with open(f'clickhouse/{i * 10}.sql', 'w+') as f:
        print(query(f'ints{i * 10}', ch=True), file=f)


for i in range(1, 17):
    with open(f'duckdb/{i}.sql', 'w+') as f:
        print(threads_query(i, 'ints100'), file=f)
