COPY catalog_sales FROM 'PATHVAR/data/tpcds/sf1/data/1_catalog_sales.csv' (FORMAT 'csv', quote '"', header 0, delimiter ',');
COPY customer FROM 'PATHVAR/data/tpcds/sf1/data/22_customer.csv' (FORMAT 'csv', quote '"', header 0, delimiter ',');
