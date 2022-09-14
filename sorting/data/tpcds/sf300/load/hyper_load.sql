-- COPY catalog_sales FROM 'PATHVAR/data/tpcds/sf300/data/1_catalog_sales.csv' (FORMAT 'csv', quote '"', header 0, delimiter ',');
COPY customer FROM '/sorting_data/tpcds/sf300/data/22_customer.csv' (FORMAT 'csv', quote '"', header 0, delimiter ',');
