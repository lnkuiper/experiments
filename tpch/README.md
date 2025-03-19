# TPC-H
Requirements are Python 3 with `pip` and the following packages:
 * `duckdb`
 * `tqdm`
 * `timeout_decorator`
 * `numpy`
 * `pandas`
 * `matplotlib`
 * `seaborn`

NOTE: HyPer is proprietary software; therefore, this README will only explain how to run DuckDB.

## Data Generation
Run the following:
```sh
python3 dbgen.py 1000
cp tpch-sf1000.db benchmark/duckdb/mydb.duckdb
```

## Running the Benchmark
Run the following:
```sh
python3 benchmark/duckdb/run.py
```
This will write results to `results/duckdb.duckdb`.

You may want to delete old results first by doing:
```sh
rm results/*
```

## Plotting
Open the `notesbooks/results.ipynb` using `jupyter-lab` and step through the cells.