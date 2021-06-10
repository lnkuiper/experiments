COPY INTO catalog_sales FROM '/sorting_data/tpcds/sf100/data/1_catalog_sales.csv' USING DELIMITERS ',', E'\n', '"' NULL AS '';
COPY INTO customer FROM '/sorting_data/tpcds/sf100/data/22_customer.csv' USING DELIMITERS ',', E'\n', '"' NULL AS '';
