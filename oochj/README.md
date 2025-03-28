# Saving Private Hash Join Sections 8 & 9 (Hash Join and Pipelined External Joins)
Requirements are Python 3 with `pip` and the following packages:
 * `duckdb`
 * `tqdm`
 * `timeout_decorator`
 * `numpy`
 * `pandas`
 * `matplotlib`
 * `seaborn`
 * `tableauhyperapi`
 * `psycopg2`
 * `psycopg2-binary`

We also need these packages from `apt`:
```
cmake build-essential ninja-build postgresql
```

You can install them using:
```sh
./script/requirements.sh
```

## Data Generation
Run the following
```sh
python3 scripts/datagen.py
```

## Installation
Both DuckDB and PostgreSQL can be installed.

### DuckDB
To evaluate the different multi-operator memory control policies, DuckDB must be compiled from source.
Go to, e.g., your home directory, and run the following
```sh
git clone https://github.com/duckdb/duckdb
cd duckdb
git checkout 5f5512b827df6397afd31daedb4bbdee76520019
```
This clones DuckDB and checks out using the v1.2.0 commit hash.

The multi-operator memory control policies are in the `policies.diff` file in this directory.
You can `git apply` this file to the repository we cloned and checked out above.
Enable one of the policies in `src/storage/temporary_memory_manager.cpp`, and compile using the following:
```sh
GEN=ninja BUILD_PYTHON=1 make
```
Note that DuckDB must be compiled from source once for every policy (and run experiments using each compiled version).

You may want to uninstall the DuckDB python package before building it from scratch with the following:
```sh
cd tools/pythonpkg
./clean.sh
```

## HyPer
HyPer is installed above through pip (`tableauhyperapi`).

### PostgreSQL
To install PostgreSQL, run the following:
```sh
cd benchmark/postgresql
./install.sh
./init.sh
```
Copy configuration file `postgresql.conf` to the initialized database directory.

## Umbra
To install Umbra, run the following under `benchmark/umbra`:
```sh
wget https://db.in.tum.de/~neumann/umbra.tar.xz
tar -xf umbra.tar.xz
```

## Running the Benchmark
To run DuckDB, run the following:
```sh
python3 benchmark/duckdb/run.py
```
You will find the results in `results/duckdb/duckdb`.
For each policy, rename the created `duckdb.duckdb` file under `results` to the name of the policy, e.g., `equity.duckdb`.

To run HyPer, run the following:
```sh
python3 benchmark/hyper/run.py
```
You will find the results in `results/hyper.duckdb`.

NOTE: a 1000-second timeout is imposed, but long-running queries cannot be interrupted in HyPer.
Therefore, running this benchmark with HyPer requires a significant amount of hand-holding.
After a query times out, all subsequent queries will throw an error, causing `results/hyper.duckdb` to be filled with error codes.
To address this, stop the process, delete the erroneous error codes in from `results/hyper.duckdb` using the `duckdb` CLI or with the `duckdb` Python package, and restart the process again (it will continue where it left off).

To run PostgreSQL, make sure that the server is running using the `start.sh` script in `benchmark/postgresql`, then run the following:
```sh
python3 benchmark/postgresql/run.py
```
You will find the results in `results/postgresql.duckdb`.

To run Umbra, run the following:
```sh
python3 benchmark/hyper/run.py
```
You will find the results in `results/umbra.duckdb`.

NOTE: Umbra does not graciously handle out-of-memory cases, and may get killed by the OS.
Therefore, running this benchmark with Umbra requires a significant amount of hand-holding.
Error codes are written correctly to `results/umbra.duckdb`, but you will have to restart the process after the OS kills it.

### Cached results
You may want to delete old results first by doing:
```sh
rm results/*
```
As the benchmark runner will use the results directory to check which benchmarks have already been run.

## Plotting (Figures 3 & 4)
Open the `notebooks/results.ipynb` using `jupyter-lab` and step through the cells to generate `figures/join.eps` (Figure 3) and `figures/pipeline.eps` (Figure 4).
