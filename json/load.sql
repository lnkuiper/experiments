COPY customer_json FROM '/Users/laurens/git/experiments/json/data/customer.json' WITH (FORMAT CSV);
COPY lineitem_json FROM '/Users/laurens/git/experiments/json/data/lineitem.json' WITH (FORMAT CSV);
COPY nation_json FROM '/Users/laurens/git/experiments/json/data/nation.json' WITH (FORMAT CSV);
COPY orders_json FROM '/Users/laurens/git/experiments/json/data/orders.json' WITH (FORMAT CSV);
COPY part_json FROM '/Users/laurens/git/experiments/json/data/part.json' WITH (FORMAT CSV);
COPY partsupp_json FROM '/Users/laurens/git/experiments/json/data/partsupp.json' WITH (FORMAT CSV);
COPY region_json FROM '/Users/laurens/git/experiments/json/data/region.json' WITH (FORMAT CSV);
COPY supplier_json FROM '/Users/laurens/git/experiments/json/data/supplier.json' WITH (FORMAT CSV);