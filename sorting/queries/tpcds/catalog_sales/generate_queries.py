columns = ['cs_sold_time_sk', 'cs_sold_date_sk', 'cs_ship_date_sk', 'cs_bill_customer_sk', 'cs_bill_cdemo_sk', 'cs_bill_hdemo_sk', 'cs_bill_addr_sk', 'cs_ship_customer_sk', 'cs_ship_cdemo_sk', 'cs_ship_hdemo_sk', 'cs_ship_addr_sk', 'cs_call_center_sk', 'cs_catalog_page_sk', 'cs_ship_mode_sk', 'cs_warehouse_sk', 'cs_item_sk', 'cs_promo_sk', 'cs_order_number', 'cs_quantity', 'cs_wholesale_cost', 'cs_list_price', 'cs_sales_price', 'cs_ext_discount_amt', 'cs_ext_sales_price', 'cs_ext_wholesale_cost', 'cs_ext_list_price', 'cs_ext_tax', 'cs_coupon_amt', 'cs_ext_ship_cost', 'cs_net_paid', 'cs_net_paid_inc_tax', 'cs_net_paid_inc_ship', 'cs_net_paid_inc_ship_tax', 'cs_net_profit']

# increase the amount of payload columns
for i in range(1, len(columns) + 1):
    with open(f'sql/payload{i}.sql', 'w+') as f:
        print('CREATE TEMPORARY TABLE output AS SELECT ' + ', '.join(columns[:i]) + ' FROM catalog_sales ORDER BY cs_quantity, cs_item_sk;', file=f)
    with open(f'clickhouse/payload{i}.sql', 'w+') as f:
        print('CREATE TEMPORARY TABLE output ENGINE = Memory AS SELECT ' + ', '.join(columns[:i]) + ' FROM catalog_sales ORDER BY cs_quantity, cs_item_sk;', file=f)

# increase the amount of sorting columns
for i in range(1, len(columns) + 1):
    with open(f'sql/sorting{i}.sql', 'w+') as f:
        print('CREATE TEMPORARY TABLE output AS SELECT * FROM catalog_sales ORDER BY ' + ', '.join(columns[:i]) + ';', file=f)
    with open(f'clickhouse/sorting{i}.sql', 'w+') as f:
        print('CREATE TEMPORARY TABLE output ENGINE = Memory AS SELECT * FROM catalog_sales ORDER BY ' + ', '.join(columns[:i]) + ';', file=f)
