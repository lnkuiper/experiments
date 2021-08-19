import duckdb
import subprocess
import os

def dsdgen(sf):
    print(f"DSDGEN SF{sf}")
    con = duckdb.connect(f"tpcds_sf{sf}.db")
    con.execute("PRAGMA threads=4")
    con.execute("PRAGMA memory_limit='10GB'")
    con.execute(f"CALL dsdgen(sf={sf});")
    con.execute(f"COPY (SELECT * FROM customer ORDER BY random()) TO 'sf{sf}/data/22_customer.csv' ( DELIMITER ',' );")
    if sf != 300:
        con.execute(f"COPY (SELECT * FROM catalog_sales ORDER BY random()) TO 'sf{sf}/data/1_catalog_sales.csv' ( DELIMITER ',' );")
    con.close()
    subprocess.run(f"rm -rf tpcds_sf{sf}.db*", shell=True)

def main():
    for sf in [1, 10, 100]:
        if "22_customer.csv" in os.listdir(f"sf{sf}/data/") and "1_catalog_sales.csv" in os.listdir(f"sf{sf}/data/"):
            continue
        dsdgen(sf)
        break

if __name__ == '__main__':
    main()

