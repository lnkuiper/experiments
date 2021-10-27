# Sorting Simulation
Sorting simulation to quantify the how the different techniques used in DuckDB's sorting implementation contributed to its performance.

## Simulation
Run `make` for an optimized build, or `make debug` for a debug build.
Parameters can be changed in the `main()` function at the bottom of the `simulation.cpp` script.
The current parameters can are set to take up less than 16GB of memory.
The script is run with `./simulation`, which generates CSV files.

## Analysis
Plots were created in a Jupyter notebook.
The relevant Python packages can be installed with:
```bash
python3 -m pip install notebook matplotlib seaborn duckdb pandas --user
```

Run `jupyter-notebook` (or `jupyter notebook` depending on the OS), then select the `analysis.ipynb` notebook and run all cells to create the plots.
