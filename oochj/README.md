# Hash Join and Multi-Operator Memory Control
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
For each policy, rename the created `duckdb.duckdb` file under `results` to the name of the policy, e.g., `equity.duckdb`.

To run HyPer, run the following:
```sh
python3 benchmark/hyper/run.py
```

To run PostgreSQL, make sure that the server is running using the `start.sh` script in `benchmark/postgresql`, then run the following:
```sh
python3 benchmark/postgresql/run.py
```

To run Umbra, run the following:
```sh
python3 benchmark/hyper/run.py
```

You may want to delete old results first by doing:
```sh
rm results/*
```

## Plotting
Open the `notebooks/results.ipynb` using `jupyter-lab` and step through the cells.