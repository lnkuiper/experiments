table = 'customer'

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

def query(key_columns, payload_columns, table):
    agg_cols = ', '.join([f'count({c})' for c in payload_columns])
    select_cols = ', '.join(payload_columns)
    order_clause = ', '.join(key_columns)
    return f'SELECT {agg_cols} FROM (SELECT {select_cols} FROM {table} ORDER BY {order_clause} offset 1) sq;'

# increase the amount of int payload columns
#key_columns = ['c_birth_year', 'c_birth_month', 'c_birth_day']
#for i in range(1, len(int_columns) + 1):
#    payload_columns = int_columns[:i]
#    with open(f'sql/int_payload{i}.sql', 'w+') as f:
#        print(query(key_columns, payload_columns, table), file=f)
#    with open(f'pandas/int_payload{i}.sql', 'w+') as f:
#        print(','.join(payload_columns), file=f)
#        print(','.join(key_columns), file=f)

# increase the amount of varchar payload columns
#key_columns = ['c_birth_year', 'c_birth_month', 'c_birth_day']
#for i in range(1, len(varchar_columns) + 1):
#    payload_columns = varchar_columns[:i]
#    with open(f'sql/varchar_payload{i}.sql', 'w+') as f:
#        print(query(key_columns, payload_columns, table), file=f)
#    with open(f'pandas/varchar_payload{i}.sql', 'w+') as f:
#        print(','.join(payload_columns), file=f)
#        print(','.join(key_columns), file=f)

key_columns = ['c_last_name', 'c_first_name']   
payload_columns = ['c_customer_sk']
with open('sql/sort_strings.sql', 'w+') as f:
    print(query(key_columns, payload_columns, table), file=f)
with open('pandas/sort_strings.sql', 'w+') as f:
    print(','.join(payload_columns), file=f)
    print(','.join(key_columns), file=f)

key_columns = ['c_birth_year', 'c_birth_month', 'c_birth_day']
with open('sql/sort_ints.sql', 'w+') as f:
    print(query(key_columns, payload_columns, table), file=f)
with open('pandas/sort_ints.sql', 'w+') as f:
    print(','.join(payload_columns), file=f)
    print(','.join(key_columns), file=f)

