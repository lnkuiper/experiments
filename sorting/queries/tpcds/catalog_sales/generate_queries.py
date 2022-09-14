table = 'catalog_sales'

columns = [
    'cs_sold_time_sk',
    'cs_sold_date_sk',
    'cs_ship_date_sk',
    'cs_bill_customer_sk',
    'cs_bill_cdemo_sk',
    'cs_bill_hdemo_sk',
    'cs_bill_addr_sk',
    'cs_ship_customer_sk',
    'cs_ship_cdemo_sk',
    'cs_ship_hdemo_sk',
    'cs_ship_addr_sk',
    'cs_call_center_sk',
    'cs_catalog_page_sk',
    'cs_ship_mode_sk',
    'cs_warehouse_sk',
    'cs_item_sk',
    'cs_promo_sk',
    'cs_order_number',
    'cs_quantity',
    'cs_wholesale_cost',
    'cs_list_price',
    'cs_sales_price',
    'cs_ext_discount_amt',
    'cs_ext_sales_price',
    'cs_ext_wholesale_cost',
    'cs_ext_list_price',
    'cs_ext_tax',
    'cs_coupon_amt',
    'cs_ext_ship_cost',
    'cs_net_paid',
    'cs_net_paid_inc_tax',
    'cs_net_paid_inc_ship',
    'cs_net_paid_inc_ship_tax',
    'cs_net_profit'
]

def query(key_columns, payload_columns, table):
    agg_cols = ', '.join([f'count({c})' for c in payload_columns])
    select_cols = ', '.join(payload_columns)
    order_clause = ', '.join(key_columns)
    return f'SELECT {agg_cols} FROM (SELECT {select_cols} FROM {table} ORDER BY {order_clause} offset 1) sq;'

# increase the amount of payload columns
#key_columns = ['cs_quantity', 'cs_item_sk']
#for i in range(1, len(columns) + 1):
#    payload_columns = columns[:i]
#    with open(f'sql/payload{i}.sql', 'w+') as f:
#        print(query(key_columns, payload_columns, table), file=f)
#    with open(f'pandas/payload{i}.sql', 'w+') as f:
#        print(','.join(payload_columns), file=f)
#        print(','.join(key_columns), file=f)

# increase the amount of sorting columns
key_columns = ['cs_warehouse_sk', 'cs_ship_mode_sk', 'cs_promo_sk', 'cs_quantity']
payload_columns = ['cs_item_sk']
for i in range(1, 5):
    key_columns = columns[:i]
    with open(f'sql/sorting{i}.sql', 'w+') as f:
        print(query(key_columns, payload_columns, table), file=f)
    with open(f'pandas/sorting{i}.sql', 'w+') as f:
        print(','.join(payload_columns), file=f)
        print(','.join(key_columns), file=f)

