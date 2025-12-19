---
title: "NetCBS: creating network measures using CBS networks (POPNET) in the RA "
date: 2025-12-20
# post image
image: "images/tutorial-10/planet-volumes-n90vqb47E7M-unsplash.jpg"
# author
author: "Javier Garcia-Bernardo and Evgeniia Krichever"
# post type (regular/featured)
type: "regular"
# meta description
description: "Creating network measures using CBS networks (POPNET)"
# post draft
draft: true

####################### Footer (always include on every page !!!) #########################
footer:
    image: "images/logos/odissei_logo.svg"
    
---
Registry data from the Central Bureau of Statistics (CBS) in the Netherlands contains rich information on the social context of individuals: family, classmates, neighbors, housemates, and colleagues. These data allow researchers to study how a person’s embeddedness in social networks affects outcomes in health, education, and labor markets. CBS makes these data available through the network files. Access requires permission; see the [CBS microdata website](https://www.cbs.nl/microdata) for details.


## Why `netcbs`?

Analyzing POPNET files directly is difficult because:

- files are extremely large (hundreds of millions of rows),
- multiple network tables must be merged,
- relationship filtering and aggregation logic is complex.


## Why `netcbs`?

Analyzing POPNET files directly is difficult because:

- files are extremely large (hundreds of millions of rows),
- multiple network tables must be merged,
- relationship filtering and aggregation logic is complex.

The `netcbs` library simplifies this by providing a **query language** that describes network paths declaratively. The library handles file loading, joins, and aggregation automatically.

## Installation

Install the package inside the RA environment (see [here](https://github.com/sodascience/cbs_python) instructions on setting up an RA environment):

```bash
pip install netcbs
```


## Example use case

Suppose you want to study how a child’s educational outcomes relate to the **average income and age of the parents of their classmates**.

### Required input data

#### 1. Sample data (`df_sample`)
Your main population of interest (children):

    RINPERSOON
    -----------
    1312231231
    2234523452
    2345234333
    4425345234
    ...


#### 2. Aggregation data (`df_agg`)
Characteristics of alters (parents):

    RINPERSOON   Income   Age
    ------------------------
    2435235880   30000    45
    8438423423   40000    32
    2345234333   50000    41


This must contain:
- `RINPERSOON`
- all variables listed in the query (`Income`, `Age`)



### Network data
You **do not** need to load network files yourself.

`netcbs` automatically uses:
- `FAMILIENETWERKTAB` (parent–child links)
- `KLASGENOTENNETWERKTAB` (schoolmate links)

## Running the analysis

```python
    import netcbs

    query = "[Income, Age] -> Family[301] -> Schoolmates[all] -> sample"

    df_result = netcbs.transform(
        query,
        df_sample=df_sample,
        df_agg=df_agg,
        year=2021,
        cbsdata_path="G:/Bevolking",
        return_pandas=False,
        agg_funcs=("avg", "sum", "count"),
        format_file="parquet", #for faster loading, but requires CBS to keep updating the parquet files, use "csv" otherwise
    )
```

### Output
The result is a **Polars DataFrame** with one row per person in `df_sample`. All sample rows are preserved; if no network links are found, aggregated values are `null`. Use `return_pandas=True` to get a Pandas DataFrame instead.      

## Understanding the query
```
    [Income, Age] -> Family[301] -> Schoolmates[all] -> sample
```

- `[Income, Age]`  
  Variables to aggregate (from `df_agg`).

- `Family[301]`  
  Parent–child relationship (code `301`).  
  Valid codes are available via:
  
      netcbs.context2types
      netcbs.codebook

- `Schoolmates[all]`  
  Include all schoolmate relationship types.

- `sample`  
  Must always be the final segment.

## Parameters overview

- `agg_funcs`  
  Supported values: `"avg"`, `"sum"`, `"count"`, `"min"`, `"max"`, `"stddev_samp"`.

- `year`  
  CBS data year.

- `cbsdata_path`  
  Base directory of CBS microdata. No need to specify

- `format_file`  
  `"parquet"` (recommended) or `"csv"`.

## More examples

See the Jupyter notebook with additional examples: [https://github.com/sodascience/netCBS/blob/main/tutorial_netCBS.ipynb](https://github.com/sodascience/netCBS/blob/main/tutorial_netCBS.ipynb)

## Citation
```
    Garcia-Bernardo, Javier (2024).
    netCBS: A Python library to efficiently create network measures using CBS networks in the RA (0.1).
    Zenodo. https://doi.org/10.5281/zenodo.13908120
```


