# netCBS
Package to efficiently create network measures using CBS networks (POPNET) in the RA


## Installation

## Only for testing locally: create synthetic data for year 2021 (1M links)
```bash
python3 create_synthetic_data.py
```

## Create network measures (e.g. the income of the parents (link type 301) of student's classmates)
```python
query =  "Sample -> Schoolmates[all] -> Family[301] -> Income"
df = netcbs.transform(query, 
                     df_sample = df_sample,  # dataset with the sample to study
                     df_agg = df_agg, # dataset with the income variable
                     year=2021, # year to study
                     cbsdata_path='cbsdata/Bevolking', # path to the CBS data, in this example is synthetic data locally 
                     agg_func=pl.mean, # calculate the average
                     return_pandas=False, # returns a pandas dataframe instead of a polars dataframe
                     lazy=True # use polars lazy evaluation (faster/less memory usage)
                     )

```

See notebook `run_netCBS.ipynb` for more information 



## Contributing

Contributions are what make the open source community an amazing place
to learn, inspire, and create. Any contributions you make are **greatly
appreciated**.

Please refer to the
[CONTRIBUTING](https://github.com/sodascience/netcbs/blob/main/CONTRIBUTING.md)
file for more information on issues and pull requests.

## License and citation

The package `netCBS` is published under an GPL-3.0 license. When using `netCBS` for academic work, please cite:

    TODO


## Contact

This project is developed and maintained by the [ODISSEI Social Data
Science (SoDa)](https://odissei-data.nl/nl/soda/) team.

<img src="soda_logo.png" alt="SoDa logo" width="250px"/>

Do you have questions, suggestions, or remarks? File an issue in the issue
tracker or feel free to contact the team via
https://odissei-data.nl/en/using-soda/.
