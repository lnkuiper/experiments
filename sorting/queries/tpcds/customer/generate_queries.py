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

int_col_idxs = [12, 13, 14, 1, 3, 4, 5, 6, 7, 18]
varchar_col_idxs = [2, 8, 9, 10, 11, 15, 16, 17]

# increase the amount of sorting columns
for i in range(1, len(int_columns) + 1):
    with open(f'sql/int{i}.sql', 'w+') as f:
        print('CREATE TEMPORARY TABLE output AS SELECT * FROM customer ORDER BY ' + ', '.join(int_columns[:i]) + ';', file=f)
    with open(f'clickhouse/int{i}.sql', 'w+') as f:
        print('CREATE TABLE output ENGINE = File(Native) AS SELECT * FROM customer ORDER BY ' + ', '.join(int_columns[:i]) + ';', file=f)
    with open(f'gnu/int{i}.sh', 'w+') as f:
        print('sort -t, ' + ' '.join([f'-k{col_idx},{col_idx}n' for col_idx in int_col_idxs[:i]]) + ' ../../data/tpcds/sf$sf$/data/22_customer.csv', file=f, end='')

# increase the amount of sorting columns
for i in range(1, len(varchar_columns) + 1):
    with open(f'sql/varchar{i}.sql', 'w+') as f:
        print('CREATE TEMPORARY TABLE output AS SELECT * FROM customer ORDER BY ' + ', '.join(varchar_columns[:i]) + ';', file=f)
    with open(f'clickhouse/varchar{i}.sql', 'w+') as f:
        print('CREATE TABLE output ENGINE = File(Native) AS SELECT * FROM customer ORDER BY ' + ', '.join(varchar_columns[:i]) + ';', file=f)
    with open(f'gnu/varchar{i}.sh', 'w+') as f:
        print('sort -t, ' + ' '.join([f'-k{col_idx},{col_idx}n' for col_idx in varchar_col_idxs[:i]]) + ' ../../data/tpcds/sf$sf$/data/22_customer.csv', file=f, end='')
