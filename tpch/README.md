# Saving Private Hash Join Section 10 (TPC-H)
Requirements are Python 3 with `pip` and the following packages:
 * `duckdb`
 * `tqdm`
 * `timeout_decorator`
 * `numpy`
 * `pandas`
 * `matplotlib`
 * `seaborn`
 * `tableauhyperapi`

## Data Generation
Run the following:
```sh
python3 dbgen.py 1000
cp tpch-sf1000.db benchmark/duckdb/mydb.duckdb
```
This creates a DuckDB database file with TPC-H SF 1000.

To ingest it into HyPer, you must export the database using DuckDB
```sql
EXPORT DATABASE 'path/to/export';
```
This will create a directory containing CSV files, and a `schema.sql` and `load.sql` file.
These should be used to load the data into a HyPer database `benchmark/hyper/mydb.hyper`.

## Running the Benchmark
How to run.

### DuckDB
Run the following:
```sh
python3 benchmark/duckdb/run.py
```
You will find the results in `results/duckdb.duckdb`.

### HyPer
Run the following:
```sh
python3 benchmark/hyper/run.py
```
You will find the results in `results/hyper.duckdb`.

NOTE: a 1000-second timeout is imposed, but long-running queries cannot be interrupted in HyPer.
Therefore, running this benchmark with HyPer requires a significant amount of hand-holding.
After a query times out, all subsequent queries will throw an error, causing `results/hyper.duckdb` to be filled with error codes.
To address this, stop the process, delete the erroneous error codes in from `results/hyper.duckdb` using the `duckdb` CLI or with the `duckdb` Python package, and restart the process again (it will continue where it left off).

You may want to delete old results first by doing:
```sh
rm results/*
```

## Plotting (Figure 5)
Open the `notebooks/results.ipynb` using `jupyter-lab` and step through the cells to generate `figures/bar.eps` (Figure 5).
