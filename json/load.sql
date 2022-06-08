INSERT INTO customer_json SELECT * FROM read_json_objects('/Users/laurens/git/experiments/json/data/customer.json');
INSERT INTO lineitem_json SELECT * FROM read_json_objects('/Users/laurens/git/experiments/json/data/lineitem.json');
INSERT INTO nation_json SELECT * FROM read_json_objects('/Users/laurens/git/experiments/json/data/nation.json');
INSERT INTO orders_json SELECT * FROM read_json_objects('/Users/laurens/git/experiments/json/data/orders.json');
INSERT INTO part_json SELECT * FROM read_json_objects('/Users/laurens/git/experiments/json/data/part.json');
INSERT INTO partsupp_json SELECT * FROM read_json_objects('/Users/laurens/git/experiments/json/data/partsupp.json');
INSERT INTO region_json SELECT * FROM read_json_objects('/Users/laurens/git/experiments/json/data/region.json');
INSERT INTO supplier_json SELECT * FROM read_json_objects('/Users/laurens/git/experiments/json/data/supplier.json');