# DuckDB Sorting experiments
Sorting experiments to compare DuckDB's sorting implementation with other systems.

## Data
Random integer data is generated with `python3 randints.py` in `data/randints`.

TPC-DS data is generated with `python3 dsdgen.py` in `data/tpcds` (requires the `duckdb` Python package).
Warning: generating SF300 takes a long time, and requires \~300GB of disk space!

## Queries
Queries are generated with `python3 generate_queries.py` under `queries/randints`, `queries/tpcds/catalog_sales` and `queries/tpcds/customer`.

## Systems
DuckDB was compared to 4 other systems.

### DuckDB
The DuckDB CLI is required, as well as the Python package.
Installation details for both can be found at https://duckdb.org.

### ClickHouse
ClickHouse needs to be compiled, see the README's under `systems/clickhouse` and `systems/clickhouse/clickhouse_client`.

ClickHouse's python client is also required:
```bash
python3 -m pip install clickhouse-driver`
```

### HyPer
We manually extracted HyPer from the Tableau binary.
We are not able to disclose how we got this to work.

### Pandas
Simple installation with pip:
```bash
python3 -m pip install pandas`
```

### SQLite
SQLite comes pre-installed in most Python installs.
SQLite's CLI is also required, which can be found in most package managers (if not pre-installed in the OS already).

## Experiments
To run the experiments, first set the appropriate values in `pathvar.sh`, which include the path to this directory, and the location of the `duckdb` binary executable.

The experiments are run with `./run.sh`.
This script can be modified to run the specific scale factor or system of your liking.

Running experiments creates `results.csv` with query timings under `results/<system>/...`, and `<query_name>.sql` files under the same directory, to denote that the query has completed.

## Plots
Plots were created in a Jupyter notebook.
The relevant Python packages are installed with:
```bash
python3 -m pip install notebook matplotlib seaborn
```

Run `jupyter-notebook` (or `jupyter notebook` depending on the OS) in the `plots` folder, then select the `plots.ipynb` notebook and run all cells to create the plots.
