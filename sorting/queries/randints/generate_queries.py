with open('sql/100_asc.sql', 'w+') as f:
    print('CREATE TEMPORARY TABLE output AS SELECT * from ints100_asc ORDER BY i;', file=f, end='')
with open('clickhouse/100_asc.sql', 'w+') as f:
    print('CREATE TEMPORARY TABLE output ENGINE = Memory AS SELECT * from ints100_asc ORDER BY i;', file=f, end='')
with open('gnu/100_asc.sh', 'w+') as f:
    print('sort -t, -k1,1n ../../data/randints/data/100asc.csv', file=f, end='')

with open('sql/100_desc.sql', 'w+') as f:
    print('CREATE TEMPORARY TABLE output AS SELECT * from ints100_desc ORDER BY i;', file=f, end='')
with open('clickhouse/100_desc.sql', 'w+') as f:
    print('CREATE TEMPORARY TABLE output ENGINE = Memory AS SELECT * from ints100_desc ORDER BY i;', file=f, end='')
with open('gnu/100_asc.sh', 'w+') as f:
    print('sort -t, -k1,1n ../../data/randints/data/100desc.csv', file=f, end='')

for i in range(1, 11):
    with open(f'sql/{i * 10}.sql', 'w+') as f:
        print(f'CREATE TEMPORARY TABLE output AS SELECT * from ints{i * 10} ORDER BY i;', file=f, end='')
    with open(f'clickhouse/{i * 10}.sql', 'w+') as f:
        print(f'CREATE TABLE output ENGINE = Memory AS SELECT * from ints{i * 10} ORDER BY i;', file=f, end='')
    with open(f'gnu/{i * 10}.sh', 'w+') as f:
        print(f'sort -t, -k1,1n ../../data/randints/data/{1 * 10}.csv', file=f, end='')

for i in range(1, 17):
    with open(f'duckdb/{i}.sql', 'w+') as f:
        print(f'PRAGMA threads={i}; CREATE TEMPORARY TABLE output AS SELECT * from ints100 ORDER BY i;', file=f, end='')
