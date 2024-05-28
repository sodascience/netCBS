import polars as pl
import pandas as pd
from numpy import mean, random
from pathlib import Path
from typing import Dict, Set, Tuple, Union
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define dictionaries with appropriate types
codebook: Dict[int, str] = {
    101: 'Neighbor - 10 closest addresses',
    102: 'Neighborhood acquaintance - 20 random neighbors within 200 meters',
    201: 'Colleague',
    301: 'Parent',
    302: 'Co-parent',
    303: 'Grandparent',
    304: 'Child',
    305: 'Grandchild',
    306: 'Full sibling',
    307: 'Half sibling',
    308: 'Unknown sibling',
    309: 'Full cousin',
    310: 'Cousin',
    311: 'Aunt/Uncle',
    312: 'Partner - married',
    313: 'Partner - not married',
    314: 'Parent-in-law',
    315: 'Child-in-law',
    316: 'Sibling-in-law',
    317: 'Stepparent',
    318: 'Stepchild',
    319: 'Stepsibling',
    320: 'Married full cousin',
    321: 'Married cousin',
    322: 'Married aunt/uncle',
    401: 'Housemate',
    402: 'Housemate - institution',
    501: 'Classmate primary education',
    502: 'Classmate special education',
    503: 'Classmate secondary education',
    504: 'Classmate vocational education',
    505: 'Classmate higher professional education',
    506: 'Classmate university education'
}

context2path: Dict[str, str] = {
    'Family': 'FAMILIENETWERKTAB',
    'Colleagues': 'COLLEGANETWERKTAB',
    'Neighbors': 'BURENNETWERKTAB',
    'Schoolmates': 'KLASGENOTENNETWERKTAB',
    'Housemates': 'HUISGENOTENNETWERKTAB'
}

context2types: Dict[str, Set[int]] = {
    'Family': set(range(301, 323)),
    'Colleagues': {201},
    'Neighbors': {101, 102},
    'Schoolmates': {501, 502, 503, 504, 505, 506},
    'Housemates': {401, 402}
}

def check_context(context: str) -> Tuple[str, Set[int]]:
    """
    Check and parse the context string to retrieve context type and associated types.

    Args:
        context (str): The context string to check.

    Returns:
        Tuple[str, Set[int]]: A tuple containing the context and a set of types.
    """
    context, set_types = context.split('[')
    context = context.strip()
    set_types = set_types.replace(']', '').strip()
    if set_types == 'all':
        set_types = context2types[context]
    else:
        set_types = {int(_) for _ in set_types.split(',')}
        if set_types - context2types[context]:
            raise ValueError(f'The set types {set_types} are not valid for context {context}. Valid types are: {context2types[context]}')
    return context, set_types

def check_last_version(path: Union[str, Path]) -> int:
    """
    Check and retrieve the latest version number from the given path.

    Args:
        path (Union[str, Path]): The path to check for versions.

    Returns:
        int: The latest version number.
    """
    path = Path(path)
    versions = [int(f.stem.split('V')[-1]) for f in path.glob('*.csv')]
    if not versions:
        raise ValueError(f'No versions found in {path}')
    return max(versions)

def format_path(context: str, year: int, cbsdata_path: str = 'cbsdata/Bevolking') -> Tuple[str, Set[int]]:
    """
    Format the path for the given context and year.

    Args:
        context (str): The context to format.
        year (int): The year for the data.
        cbsdata_path (str): The base path for CBS data.

    Returns:
        Tuple[str, Set[int]]: A tuple containing the formatted path and the set of types.
    """
    context, set_types = check_context(context)
    path_context = context2path[context]
    version = check_last_version(f'{cbsdata_path}/{path_context}')
    path_context = f'{cbsdata_path}/{path_context}/{path_context}{year}V{version}.csv'
    return path_context, set_types

def transform(
    query: str, 
    df_sample: pl.DataFrame, 
    df_agg: pl.DataFrame, 
    year: int = 2020, 
    agg_func: Union[pl.Expr, callable] = pl.mean, 
    return_pandas: bool = False, 
    lazy: bool = False, 
    cbsdata_path: str = 'cbsdata/Bevolking'
) -> Union[pl.DataFrame, pd.DataFrame]:
    """
    Transform the data based on the given query and return the transformed dataframe.

    Args:
        query (str): The query string.
        df_sample (pl.DataFrame): The sample dataframe. Should contain at least columns 'RINPERSOON' and 'RINPERSOONS'.
        df_agg (pl.DataFrame): The aggregation dataframe. Should contain columns 'RINPERSOON', 'RINPERSOONS', and the aggregation column.
        year (int): The year for the data.
        agg_func (Union[pl.Expr, callable]): The aggregation function.
        return_pandas (bool): Whether to return a pandas dataframe.
        lazy (bool): Whether to use lazy evaluation. Set to False for debugging.
        cbsdata_path (str): The base path for CBS data.

    Returns:
        Union[pl.DataFrame, 'pd.DataFrame']: The transformed dataframe.
    """
    contexts = query.strip().split(' -> ')

    if len(contexts) <= 2:
        raise ValueError('Query must contain at least one link')
    if len(contexts) > 4:
        logger.warning('The query is too complex, it will take too long or potentially never finish')

    for context in contexts[1:-1]:
        check_context(context)

    validate_columns(df_sample, ['RINPERSOON', 'RINPERSOONS'])
    validate_columns(df_agg, ['RINPERSOON', 'RINPERSOONS', contexts[-1]])

    df = (prepare_dataframe(df_sample, lazy)
          .with_columns(
              pl.col('RINPERSOON').alias('RINPERSOON_sample'),
              pl.col('RINPERSOONS').alias('RINPERSOONS_sample')
          )
          )
    df_agg = prepare_dataframe(df_agg, lazy)
    if lazy:
        df_sample = pl.LazyFrame(df_sample)
    else:
        df_sample = pl.DataFrame(df_sample)

    for context in contexts[1:-1]:
        path_context, set_types = format_path(context, year, cbsdata_path)
        df_context = load_context_data(path_context, set_types, lazy)
        df = merge_context_data(df, df_context)
        if not lazy:
            logger.info(f'Context {context} added. New data size: {df.shape}')

    df = aggregate_data(df, df_agg, contexts[-1], agg_func)

    df = df_sample.join(df, on=['RINPERSOON', 'RINPERSOONS'], how='left')

    if lazy:
        df = df.collect()

    if return_pandas:
        return df.to_pandas()
    return df

def validate_columns(df: pl.DataFrame, columns: list):
    """
    Validate that the dataframe contains the specified columns.

    Args:
        df (pl.DataFrame): The dataframe to validate.
        columns (list): The list of columns to check.

    Raises:
        ValueError: If any of the columns are not found in the dataframe.
    """
    for column in columns:
        if column not in df.columns:
            raise ValueError(f'The dataframe does not contain the column "{column}"')

def prepare_dataframe(df: pl.DataFrame, lazy: bool) -> pl.DataFrame:
    """
    Prepare the dataframe by removing duplicates and converting to lazy if needed.

    Args:
        df (pl.DataFrame): The dataframe to prepare.
        lazy (bool): Whether to convert the dataframe to lazy.

    Returns:
        pl.DataFrame: The prepared dataframe.
    """
    df = pl.DataFrame(df)
    if 'RINPERSOON' in df.columns and 'RINPERSOONS' in df.columns:
        if df[['RINPERSOON', 'RINPERSOONS']].is_duplicated().any():
            df = df.unique(subset=['RINPERSOON', 'RINPERSOONS'])
            logger.info('The dataframe contained duplicated entries, they were removed. New data size: %s', df.shape)
    return pl.LazyFrame(df) if lazy else df

def load_context_data(path: str, set_types: Set[int], lazy: bool) -> pl.DataFrame:
    """
    Load the context data from the specified path.

    Args:
        path (str): The path to the context data.
        set_types (Set[int]): The set of types to filter.
        lazy (bool): Whether to use lazy evaluation.

    Returns:
        pl.DataFrame: The loaded context data.
    """
    df_context = pl.scan_csv(path) if lazy else pl.read_csv(path)
    return df_context.filter(pl.col('RELATIE').is_in(set_types)).select(['RINPERSOON', 'RINPERSOONS', 'RINPERSOONRELATIE', 'RINPERSOONSRELATIE'])

def merge_context_data(df: pl.DataFrame, df_context: pl.DataFrame) -> pl.DataFrame:
    """
    Merge the context data with the main dataframe.

    Args:
        df (pl.DataFrame): The main dataframe.
        df_context (pl.DataFrame): The context dataframe.

    Returns:
        pl.DataFrame: The merged dataframe.
    """
    df = df.join(df_context, on=['RINPERSOON', 'RINPERSOONS'], how='inner').select(['RINPERSOON_sample', 'RINPERSOONS_sample', 'RINPERSOONRELATIE', 'RINPERSOONSRELATIE'])
    return df.rename({'RINPERSOONRELATIE': 'RINPERSOON', 'RINPERSOONSRELATIE': 'RINPERSOONS'})

def aggregate_data(df: pl.DataFrame, df_agg: pl.DataFrame, agg_col: str, agg_func: Union[pl.Expr, callable]) -> pl.DataFrame:
    """
    Aggregate the data based on the specified column and function.

    Args:
        df (pl.DataFrame): The main dataframe.
        df_agg (pl.DataFrame): The aggregation dataframe.
        agg_col (str): The column to aggregate.
        agg_func (Union[pl.Expr, callable]): The aggregation function.

    Returns:
        pl.DataFrame: The aggregated dataframe.
    """
    df = df.join(df_agg, on=['RINPERSOON', 'RINPERSOONS'], how='inner')
    df = df.groupby(['RINPERSOON_sample', 'RINPERSOONS_sample']).agg(agg_func(agg_col)).rename({'RINPERSOON_sample': 'RINPERSOON', 'RINPERSOONS_sample': 'RINPERSOONS'})
    return df
