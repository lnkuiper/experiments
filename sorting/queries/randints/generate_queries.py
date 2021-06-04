with open('sql/100_asc.sql', 'w+') as f:
    print('CREATE TABLE output AS SELECT * from ints100_asc ORDER BY i;', file=f)
with open('clickhouse/100_asc.sql', 'w+') as f:
    print('CREATE TABLE output ENGINE = Memory AS SELECT * from ints100_asc ORDER BY i;', file=f)

with open('sql/100_desc.sql', 'w+') as f:
    print('CREATE TABLE output AS SELECT * from ints100_desc ORDER BY i;', file=f)
with open('clickhouse/100_desc.sql', 'w+') as f:
    print('CREATE TABLE output ENGINE = Memory AS SELECT * from ints100_desc ORDER BY i;', file=f)

for i in range(1, 11):
    with open(f'sql/{i * 10}.sql', 'w+') as f:
        print(f'CREATE TABLE output AS SELECT * from ints{i * 10} ORDER BY i;', file=f)
    with open(f'clickhouse/{i * 10}.sql', 'w+') as f:
        print(f'CREATE TABLE output ENGINE = Memory AS SELECT * from ints{i * 10} ORDER BY i;', file=f)
