COPY item FROM '/sorting_data/tpcds/sf10/data/19_item.csv' (FORMAT 'csv', quote '"', header 0, delimiter ',');
COPY income_band FROM '/sorting_data/tpcds/sf10/data/18_income_band.csv' (FORMAT 'csv', quote '"', header 0, delimiter ',');
COPY reason FROM '/sorting_data/tpcds/sf10/data/17_reason.csv' (FORMAT 'csv', quote '"', header 0, delimiter ',');
COPY time_dim FROM '/sorting_data/tpcds/sf10/data/16_time_dim.csv' (FORMAT 'csv', quote '"', header 0, delimiter ',');
COPY ship_mode FROM '/sorting_data/tpcds/sf10/data/15_ship_mode.csv' (FORMAT 'csv', quote '"', header 0, delimiter ',');
COPY warehouse FROM '/sorting_data/tpcds/sf10/data/14_warehouse.csv' (FORMAT 'csv', quote '"', header 0, delimiter ',');
COPY date_dim FROM '/sorting_data/tpcds/sf10/data/13_date_dim.csv' (FORMAT 'csv', quote '"', header 0, delimiter ',');
COPY store_sales FROM '/sorting_data/tpcds/sf10/data/0_store_sales.csv' (FORMAT 'csv', quote '"', header 0, delimiter ',');
COPY store_returns FROM '/sorting_data/tpcds/sf10/data/10_store_returns.csv' (FORMAT 'csv', quote '"', header 0, delimiter ',');
COPY catalog_sales FROM '/sorting_data/tpcds/sf10/data/1_catalog_sales.csv' (FORMAT 'csv', quote '"', header 0, delimiter ',');
COPY customer_demographics FROM '/sorting_data/tpcds/sf10/data/12_customer_demographics.csv' (FORMAT 'csv', quote '"', header 0, delimiter ',');
COPY customer FROM '/sorting_data/tpcds/sf10/data/22_customer.csv' (FORMAT 'csv', quote '"', header 0, delimiter ',');
COPY web_returns FROM '/sorting_data/tpcds/sf10/data/3_web_returns.csv' (FORMAT 'csv', quote '"', header 0, delimiter ',');
COPY web_site FROM '/sorting_data/tpcds/sf10/data/23_web_site.csv' (FORMAT 'csv', quote '"', header 0, delimiter ',');
COPY store FROM '/sorting_data/tpcds/sf10/data/20_store.csv' (FORMAT 'csv', quote '"', header 0, delimiter ',');
COPY catalog_returns FROM '/sorting_data/tpcds/sf10/data/4_catalog_returns.csv' (FORMAT 'csv', quote '"', header 0, delimiter ',');
COPY call_center FROM '/sorting_data/tpcds/sf10/data/21_call_center.csv' (FORMAT 'csv', quote '"', header 0, delimiter ',');
COPY web_sales FROM '/sorting_data/tpcds/sf10/data/2_web_sales.csv' (FORMAT 'csv', quote '"', header 0, delimiter ',');
COPY inventory FROM '/sorting_data/tpcds/sf10/data/5_inventory.csv' (FORMAT 'csv', quote '"', header 0, delimiter ',');
COPY promotion FROM '/sorting_data/tpcds/sf10/data/7_promotion.csv' (FORMAT 'csv', quote '"', header 0, delimiter ',');
COPY catalog_page FROM '/sorting_data/tpcds/sf10/data/6_catalog_page.csv' (FORMAT 'csv', quote '"', header 0, delimiter ',');
COPY web_page FROM '/sorting_data/tpcds/sf10/data/8_web_page.csv' (FORMAT 'csv', quote '"', header 0, delimiter ',');
COPY household_demographics FROM '/sorting_data/tpcds/sf10/data/9_household_demographics.csv' (FORMAT 'csv', quote '"', header 0, delimiter ',');
COPY customer_address FROM '/sorting_data/tpcds/sf10/data/11_customer_address.csv' (FORMAT 'csv', quote '"', header 0, delimiter ',');