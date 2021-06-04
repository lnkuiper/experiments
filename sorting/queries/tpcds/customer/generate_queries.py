int_columns = ['c_customer_sk', 'c_current_cdemo_sk', 'c_current_hdemo_sk', 'c_current_addr_sk', 'c_first_shipto_date_sk', 'c_first_sales_date_sk', 'c_birth_day', 'c_birth_month', 'c_birth_year', 'c_last_review_date_sk']
varchar_columns = ['c_customer_id', 'c_salutation', 'c_first_name', 'c_last_name', 'c_preferred_cust_flag', 'c_birth_country', 'c_login', 'c_email_address']

# increase the amount of sorting columns
for i in range(1, len(int_columns) + 1):
    with open(f'sql/int{i}.sql', 'w+') as f:
        print('CREATE TABLE output AS SELECT * FROM customer ORDER BY ' + ', '.join(int_columns[:i]) + ';', file=f)
    with open(f'clickhouse/int{i}.sql', 'w+') as f:
        print('CREATE TABLE output ENGINE = Memory AS SELECT * FROM customer ORDER BY ' + ', '.join(int_columns[:i]) + ';', file=f)

# increase the amount of sorting columns
for i in range(1, len(varchar_columns) + 1):
    with open(f'sql/varchar{i}.sql', 'w+') as f:
        print('CREATE TABLE output AS SELECT * FROM customer ORDER BY ' + ', '.join(varchar_columns[:i]) + ';', file=f)
    with open(f'clickhouse/varchar{i}.sql', 'w+') as f:
        print('CREATE TABLE output ENGINE = Memory AS SELECT * FROM customer ORDER BY ' + ', '.join(varchar_columns[:i]) + ';', file=f)
