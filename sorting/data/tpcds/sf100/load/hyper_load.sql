COPY catalog_sales FROM '/sorting_data/tpcds/sf100/data/1_catalog_sales.csv' (FORMAT 'csv', quote '"', header 0, delimiter ',');
COPY customer FROM '/sorting_data/tpcds/sf100/data/22_customer.csv' (FORMAT 'csv', quote '"', header 0, delimiter ',');
