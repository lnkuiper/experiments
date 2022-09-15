import duckdb

full_size = 100000000

con = duckdb.connect()
con.execute(f"create table floats as select cast((random()::FLOAT * 2::FLOAT - 1::FLOAT) * {full_size}::FLOAT as float) i from range({full_size});")


con.execute("copy (select i from floats order by i) to 'data/100asc.csv';")
con.execute("copy (select i from floats order by i desc) to 'data/100desc.csv';")

for i in range(1,11):
    size = int(full_size / 10) * i
    con.execute(f"copy (select i from floats limit {size}) to 'data/{i*10}.csv';")

