CALL dbgen(sf=1);
COPY (SELECT to_json(customer) FROM customer) TO 'data/customer.json';
COPY (SELECT to_json(lineitem) FROM lineitem) TO 'data/lineitem.json';
COPY (SELECT to_json(nation) FROM nation) TO 'data/nation.json';
COPY (SELECT to_json(orders) FROM orders) TO 'data/orders.json';
COPY (SELECT to_json(part) FROM part) TO 'data/part.json';
COPY (SELECT to_json(partsupp) FROM partsupp) TO 'data/partsupp.json';
COPY (SELECT to_json(region) FROM region) TO 'data/region.json';
COPY (SELECT to_json(supplier) FROM supplier) TO 'data/supplier.json';
