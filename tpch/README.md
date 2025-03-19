# TPC-H
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
To run DuckDB, run the following:
```sh
python3 benchmark/duckdb/run.py
```
This will write results to `results/duckdb.duckdb`.

To run HyPer, run the following:
```sh
python3 benchmark/hyper/run.py
```

You may want to delete old results first by doing:
```sh
rm results/*
```

## Plotting
Open the `notebooks/results.ipynb` using `jupyter-lab` and step through the cells.