# netCBS
`netCBS` efficiently creates network-based measures using CBS POPNET network tables (e.g. family, colleagues, neighbors, schoolmates, housemates). For example: compute the average income of a person’s parents, or the average income of the parents of their classmates, using CBS network links.


## Installation

```bash
pip install netcbs
```

## Quick start

See [notebook](tutorial_netCBS.ipynb) for accessible information and examples.

The core function is `transform(query, df_sample, df_agg, ...)`.

### Inputs
- **`df_sample`**: your “ego” sample. Must contain:
  - `RINPERSOON` (unique person identifier). Note: `RINPERSOONS` must be `R`

- **`df_agg`**: the table containing variables you want to aggregate for alters reached by the network traversal. Must contain:
  - `RINPERSOON`. Note: `RINPERSOONS` must be `R`
  - all variables referenced in the query’s aggregation-variable list (e.g. `Income`, `Age`)


### Query format

A query describes:

1) **Which variables to aggregate** (first segment), and  
2) **Which network hops to traverse** (one or more context segments), ending in `sample`.

Format:

"[Var1, Var2, ...] -> ContextA[types] -> ContextB[types] -> ... -> sample"

- The first segment **must** be in square brackets: `"[Income]"` or `"[Income, Age]"`.
- Each context is one of: `Family`, `Colleagues`, `Neighbors`, `Schoolmates`, `Housemates`.
- Context type selector is either:
  - `[all]` (use all relationship codes valid for that context), or
  - `[101,102,...]` (explicit relationship codes)
- The final segment should be `sample` (case-sensitive recommended).

Example:

query = "[Income, Age] -> Family[301] -> Schoolmates[all] -> sample"

This means: find the aggregated `Income` and `Age` of **parents (301)** of the **schoolmates** of the people in the sample (`df_sample`).

## Usage
```python
import polars as pl  
import netcbs

query = "[Income, Age] -> Family[301] -> Schoolmates[all] -> sample"

df_out = netcbs.transform(
    query=query,
    df_sample=df_sample,     # must contain: RINPERSOON
    df_agg=df_agg,           # must contain: RINPERSOON, Income, Age
    year=2021,
    format_file="parquet",   # "parquet" (recommended) or "csv"
    agg_funcs=("avg", "sum", "count"),  # DuckDB aggregate function names (strings)
    return_pandas=False, 
)
```

### About `agg_funcs` (important)

`agg_funcs` must be a sequence of **DuckDB aggregate function names** as **strings**, e.g.:

- `"avg"`, `"sum"`, `"count"`, `"min"`, `"max"` (and other DuckDB aggregates)


The output columns are named:

"<func>_<Var>"

So with `agg_funcs=("avg","sum")` and `"[Income, Age]"`, you get:

- `avg_Income`, `sum_Income`, `avg_Age`, `sum_Age`


## How it works

1) **Validate query**  
   `validate_query()` checks:
   - query structure
   - `df_sample` has `RINPERSOON`
   - `df_agg` has `RINPERSOON` and all requested aggregation variables
   - each context and relationship-type selector is valid
   - (optionally) referenced CBS files exist for the requested year

2) **Resolve network files**  
   For each hop, `format_path()` selects the latest available version of the CBS network file for the requested year.
   - For `format_file="parquet"`, files are expected under a `geconverteerde data` subfolder.
   - For `format_file="csv"`, files are read with `read_csv_auto(..., delim=';')`.

3) **Traverse the network**  
   DuckDB reads each network file, filters by the requested relationship codes, and joins hop-by-hop from egos to alters.

4) **Aggregate**  
   DuckDB joins the final reached persons to `df_agg` and computes the requested aggregates, grouped by the original sample person.

5) **Join back to sample**  
   Results are left-joined back onto the sample so every sample person remains in the output (missing networks produce null aggregates).

## Contributing

Please refer to the repository’s CONTRIBUTING guide for issues and pull requests.

## License and citation

`netCBS` is published under the MIT license.  
For academic citation: Garcia-Bernardo, J. (2024). netCBS: Package to efficiently create network measures using CBS networks in the RA. (v0.1). Zenodo. https://doi.org/10.5281/zenodo.13908121

## Contact

Developed and maintained by the ODISSEI Social Data Science (SoDa) team.  
Questions or suggestions: please open an issue or contact via the ODISSEI SoDa website.

