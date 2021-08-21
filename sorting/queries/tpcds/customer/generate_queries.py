int_columns = [
    'c_birth_day',
    'c_birth_month',
    'c_birth_year',
    'c_customer_sk',
    'c_current_cdemo_sk',
    'c_current_hdemo_sk',
    'c_current_addr_sk',
    'c_first_shipto_date_sk',
    'c_first_sales_date_sk',
    'c_last_review_date_sk'
]
varchar_columns = [
    'c_customer_id',
    'c_salutation',
    'c_first_name',
    'c_last_name',
    'c_preferred_cust_flag',
    'c_birth_country',
    'c_login',
    'c_email_address'
]

# increase the amount of int sorting columns
for i in range(1, len(int_columns) + 1):
    with open(f'sql/int{i}.sql', 'w+') as f:
        print('CREATE TEMPORARY TABLE output AS SELECT * FROM customer ORDER BY ' + ', '.join(int_columns[:i]) + ';', file=f)
    with open(f'clickhouse/int{i}.sql', 'w+') as f:
        print('CREATE TABLE output ENGINE = File(Native) AS SELECT * FROM customer ORDER BY ' + ', '.join(int_columns[:i]) + ';', file=f)

# increase the amount of varchar sorting columns
for i in range(1, len(varchar_columns) + 1):
    with open(f'sql/varchar{i}.sql', 'w+') as f:
        print('CREATE TEMPORARY TABLE output AS SELECT * FROM customer ORDER BY ' + ', '.join(varchar_columns[:i]) + ';', file=f)
    with open(f'clickhouse/varchar{i}.sql', 'w+') as f:
        print('CREATE TABLE output ENGINE = File(Native) AS SELECT * FROM customer ORDER BY ' + ', '.join(varchar_columns[:i]) + ';', file=f)

# increase the amount of int payload columns
for i in range(1, len(int_columns) + 1):
    with open(f'sql/int_payload{i}.sql', 'w+') as f:
        print('CREATE TEMPORARY TABLE output AS SELECT ' + ', '.join(int_columns[:i]) + ' FROM customer ORDER BY c_birth_year, c_birth_month, c_birth_day;', file=f)
    with open(f'clickhouse/int_payload{i}.sql', 'w+') as f:
        print('CREATE TABLE output ENGINE = File(Native) AS SELECT ' + ', '.join(int_columns[:i]) + ' FROM customer ORDER BY c_birth_year, c_birth_month, c_birth_day;', file=f)

# increase the amount of varchar payload columns
for i in range(1, len(varchar_columns) + 1):
    with open(f'sql/varchar_payload{i}.sql', 'w+') as f:
        print('CREATE TEMPORARY TABLE output AS SELECT ' + ', '.join(varchar_columns[:i]) + ' FROM customer ORDER BY c_birth_year, c_birth_month, c_birth_day;', file=f)
    with open(f'clickhouse/varchar_payload{i}.sql', 'w+') as f:
        print('CREATE TABLE output ENGINE = File(Native) AS SELECT ' + ', '.join(varchar_columns[:i]) + ' FROM customer ORDER BY c_birth_year, c_birth_month, c_birth_day;', file=f)

with open('sql/sort_strings.sql', 'w+') as f:
    print('CREATE TEMPORARY TABLE output AS SELECT * FROM customer ORDER BY c_first_name, c_last_name;', file=f)
with open('clickhouse/sort_strings.sql', 'w+') as f:
    print('CREATE TABLE output ENGINE = File(Native) AS SELECT * FROM customer ORDER BY c_first_name, c_last_name;', file=f)

with open('sql/sort_ints.sql', 'w+') as f:
    print('CREATE TEMPORARY TABLE output AS SELECT * FROM customer ORDER BY c_birth_year, c_birth_month, c_birth_day;', file=f)
with open('clickhouse/sort_ints.sql', 'w+') as f:
    print('CREATE TABLE output ENGINE = File(Native) AS SELECT * FROM customer ORDER BY c_birth_year, c_birth_month, c_birth_day;', file=f)
