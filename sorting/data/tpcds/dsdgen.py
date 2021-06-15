import duckdb
import sys

def dsdgen(sf):
    print(f"DSDGEN SF{sf}");
    con = duckdb.connect(f"tpcds_sf{sf}.db")
    con.execute(f"CALL dsdgen(sf={sf})")
    con.execute(f"COPY customer TO 'sf{sf}/data/22_customer.csv' ( DELIMITER ',' );"
    con.execute(f"COPY catalog_sales TO 'sf{sf}/data/1_catalog_sales.csv' ( DELIMITER ',' );"
    con.close()

def main():
    dsdgen(sys.argv[1])

if __name__ == '__main__':
    main()
