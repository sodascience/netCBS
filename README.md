# netCBS
Package to efficiently create network measures using CBS networks (POPNET) in the RA. For example you may be interested in calculating the average income of the parents of the classmates of a student. This package allows you to do this in a fast and efficient way.

## Installation

```bash
pip install git+https://git@github.com/sodascience/netcbs.git@main
```

## Usage

See [notebook](tutorial_netCBS.ipynb) for accessible information and examples.

For testing locally: run this script to create synthetic data (1M of random edges for year 2021). The files are saved to the folder "cbsdata/Bevolking"
```bash
python3 netcbs/create_synthetitcata.py
```

### Create network measures (e.g. the average income and age of the parents (link type 301) of the classmats of children in the sample)
```python
query =  "[Income, Age] -> Family[301] -> Schoolmates[all] -> Sample"
df = netcbs.transform(query, 
                     df_sample = df_sample,  # dataset with the sample to study
                     df_agg = df_agg, # dataset with the income variable
                     year=2021, # year to study
                     cbsdata_path='G:/Bevolking', # path to the CBS data
                     agg_func=pl.mean, # calculate the average
                     return_pandas=False, # returns a pandas dataframe instead of a polars dataframe
                     lazy=True # use polars lazy evaluation (faster/less memory usage)
                     )

```

## How does the library work?
### Query system
The library uses a query system to specify the relationships between the main sample dataframe and the context data. The query consists of a series of context types separated by arrows (->), with optional relationship types in square brackets. For example, the query `"[Income] -> Family[301] -> Schoolmates[all] -> Sample"` specifies that the income of the parents of the student's classmates should be calculated based on the provided sample dataframe.

### Data used:
The library checks the latest verion of each network file for the year specified in the `transform` function. 

The library removes duplicate entries from the df_sample and df_agg dataframes, and converts them to polars for efficient.

### Transformation fo the query
The `validate_query` function (called automatically by the `transform` function) ensures that the query string is correctly formatted and that all necessary columns are present in the input dataframes. It splits the query into individual contexts and verifies each part, raising errors for any issues found.

The different network files (contexts) are merged (inner join) consecutively based on the relationship columns specified in the query. The resulting dataframe is then aggregated based on the aggregation function (e.g., pl.mean, pl.sum) specified in the `transform` function.

We recommend to use the polars lazy evaluation (lazy=True) to reduce memory usage and speed up the calculations. For debugging this can be disabled by setting lazy=False.


## Contributing
Contributions are what make the open source community an amazing place to learn, inspire, and create. Any contributions you make are **greatly appreciated**.

Please refer to the [CONTRIBUTING](https://github.com/sodascience/netcbs/blob/main/CONTRIBUTING.md) file for more information on issues and pull requests.

## License and citation

The package `netCBS` is published under an MIT license. When using `netCBS` for academic work, please cite:
```
    TODO
```

## Contact

This project is developed and maintained by the [ODISSEI Social Data
Science (SoDa)](https://odissei-data.nl/nl/soda/) team.

<img src="soda_logo.png" alt="SoDa logo" width="250px"/>

Do you have questions, suggestions, or remarks? File an issue in the issue
tracker or feel free to contact the team via
https://odissei-data.nl/en/using-soda/.
