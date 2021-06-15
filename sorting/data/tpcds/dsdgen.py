import duckdb
import sys

def dsdgen(sf):
    print(f"DSDGEN SF{sf}");
    con = duckdb.connect(f"tpcds_sf{sf}.db")
    con.execute(f"CALL dsdgen(sf={sf})")
    con.execute(f"EXPORT DATABASE 'sf{sf}/data' (FORMAT CSV, DELIMITER ',')")
    con.close()

def main():
    dsdgen(sys.argv[1])

if __name__ == '__main__':
    main()
