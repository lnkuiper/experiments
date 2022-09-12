CALL dbgen(sf=1);

COPY (SELECT to_json(customer) AS json FROM customer) TO 'data/customer.json' (QUOTE '');
COPY (SELECT to_json(lineitem) AS json FROM lineitem) TO 'data/lineitem.json' (QUOTE '');
COPY (SELECT to_json(nation) AS json FROM nation) TO 'data/nation.json' (QUOTE '');
COPY (SELECT to_json(orders) AS json FROM orders) TO 'data/orders.json' (QUOTE '');
COPY (SELECT to_json(part) AS json FROM part) TO 'data/part.json' (QUOTE '');
COPY (SELECT to_json(partsupp) AS json FROM partsupp) TO 'data/partsupp.json' (QUOTE '');
COPY (SELECT to_json(region) AS json FROM region) TO 'data/region.json' (QUOTE '');
COPY (SELECT to_json(supplier) AS json FROM supplier) TO 'data/supplier.json' (QUOTE '');

COPY (SELECT to_json(customer) AS json FROM customer) TO 'data/customer.parquet' (FORMAT 'parquet');
COPY (SELECT to_json(lineitem) AS json FROM lineitem) TO 'data/lineitem.parquet' (FORMAT 'parquet');
COPY (SELECT to_json(nation) AS json FROM nation) TO 'data/nation.parquet' (FORMAT 'parquet');
COPY (SELECT to_json(orders) AS json FROM orders) TO 'data/orders.parquet' (FORMAT 'parquet');
COPY (SELECT to_json(part) AS json FROM part) TO 'data/part.parquet' (FORMAT 'parquet');
COPY (SELECT to_json(partsupp) AS json FROM partsupp) TO 'data/partsupp.parquet' (FORMAT 'parquet');
COPY (SELECT to_json(region) AS json FROM region) TO 'data/region.parquet' (FORMAT 'parquet');
COPY (SELECT to_json(supplier) AS json FROM supplier) TO 'data/supplier.parquet' (FORMAT 'parquet');
