import duckdb

sfs = [1, 10, 100, 300]

for sf in sfs:
	con = duckdb.connect(f"tpcds_sf{sf}.db")
	con.execute(f"CALL dsdgen(sf={sf}")
	con.execute(f"EXPORT DATABASE 'sf{sf}/data' (FORMAT CSV, DELIMITER ',')")

