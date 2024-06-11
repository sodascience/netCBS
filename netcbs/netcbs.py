"""Package to create network variables (e.g. the mean income of the family members) from CBS network data (POPNET)"""

import logging
from pathlib import Path
from typing import Dict, List, Set, Tuple, TypeVar, Union, Callable

import polars as pl
import pandas as pd

# TypeVar for DataFrame types
DF = TypeVar("DF", pl.DataFrame, pl.LazyFrame)

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

def _check_context(context: str) -> Tuple[str, Set[int]]:
    """
    Check and parse the context string to retrieve context type and associated types.

    Args:
        context (str): The context string to check.

    Returns:
        Tuple[str, Set[int]]: A tuple containing the context and a set of types.
    """

    # Filter the context and the types: Family[301, 302, 303] -> Family, {301, 302, 303}
    context, var_set_types = context.split('[')
    context = context.strip()
    var_set_types = var_set_types.replace(']', '').strip()
    if var_set_types == 'all':
        set_types = context2types[context]
    else:
        set_types = {int(_) for _ in var_set_types.split(',')}
        if set_types - context2types[context]:
            raise ValueError(f'The set types {set_types} are not valid for context {context}. Valid types are: {context2types[context]}')
    return context, set_types

def _check_last_version(path: Union[str, Path]) -> int:
    """
    Check and retrieve the latest version number from the given path.

    Args:
        path (Union[str, Path]): The path to check for versions.

    Returns:
        int: The latest version number.
    """

    # The files end on V1, V2, V3, etc. We need to extract the number and return the maximum
    path = Path(path)
    versions = [int(f.stem.split('V')[-1]) for f in path.glob('*.csv')]
    if not versions:
        raise ValueError(f'No versions found in {path}')
    return max(versions)

def format_path(context: str, year: int, cbsdata_path: str = 'cbsdata/Bevolking') -> Tuple[str, Set[int]]:
    """
    Format the path for the given context and year.

    Args:
        context (str): The context to format (e.g. Family[301])
        year (int): The year for the data.
        cbsdata_path (str): The base path for CBS data.

    Returns:
        Tuple[str, Set[int]]: A tuple containing the formatted path and the set of types.
    """
    context, set_types = _check_context(context)
    name_path_context = context2path[context]
    version = _check_last_version(f'{cbsdata_path}/{name_path_context}')
    path_context = f'{cbsdata_path}/{name_path_context}/{name_path_context}{year}V{version}.csv'
    return path_context, set_types

def validate_query(
        query: str, 
        df_sample: Union[pl.DataFrame, pl.LazyFrame, pd.DataFrame],
        df_agg: Union[pl.DataFrame, pl.LazyFrame, pd.DataFrame],
        year: int = 2020,
        cbsdata_path: str = 'cbsdata/Bevolking'
        ) -> List[str]:
    """
    Validate the query and return the contexts.

    Args:
        query (str): The query string.
        df_sample (pl.DataFrame): The sample dataframe.
        df_agg (pl.DataFrame): The aggregation dataframe.
        year (int): The year for the data.
        cbsdata_path (str): The base path for CBS data.

    Returns:
        List[str]: The list of contexts.
    """
    
    contexts = query.strip().split(' -> ')[::-1]

    if len(contexts) <= 2:
        raise ValueError('Query must contain at least one link')
    if len(contexts) > 4:
        logger.warning('The query is too complex, it will take too long or potentially never finish')

    _validate_columns(df_sample, ['RINPERSOON', 'RINPERSOONS'])

    var_aggs = _format_vars_agg(contexts[-1])
    _validate_columns(df_agg, ['RINPERSOON', 'RINPERSOONS'] + var_aggs)

    for context in contexts[1:-1]:
        _ = format_path(context, year, cbsdata_path)

    

    return contexts

def transform(
    query: str, 
    df_sample: Union[pl.DataFrame, pl.LazyFrame, pd.DataFrame],
    df_agg: Union[pl.DataFrame, pl.LazyFrame, pd.DataFrame],
    year: int = 2020,
    agg_func: Union[pl.Expr, Callable] = pl.mean,
    return_pandas: bool = False,
    lazy: bool = False,
    cbsdata_path: str = 'cbsdata/Bevolking'
) -> Union[pl.DataFrame, pd.DataFrame]:
    """
    Transform the data based on the given query and return the transformed dataframe.

    Args:
        query (str): The query string. It needs to start with the variables (contained in df_agg) to be aggregated, e.g. "Income -> ". It needs to end with -> Sample".
        df_sample (pl.DataFrame): The sample dataframe. Should contain at least columns 'RINPERSOON' and 'RINPERSOONS'.
        df_agg (pl.DataFrame): The aggregation dataframe. Should contain columns 'RINPERSOON', 'RINPERSOONS', and the aggregation column.
        year (int): The year for the data.
        agg_func (Union[pl.Expr, Callable]): The aggregation function.
        return_pandas (bool): Whether to return a pandas dataframe.
        lazy (bool): Whether to use lazy evaluation. Set to False for debugging.
        cbsdata_path (str): The base path for CBS data.

    Returns:
        Union[pl.DataFrame, 'pd.DataFrame']: The transformed dataframe.
    """

    contexts = validate_query(query, df_sample, df_agg, year, cbsdata_path)

    # Prepare the dataframe with the sample data. Duplicate ID columsn to recover the sample IDs after merging
    df = (_prepare_dataframe(df_sample, lazy)
          .with_columns(
              pl.col('RINPERSOON').alias('RINPERSOON_sample'),
              pl.col('RINPERSOONS').alias('RINPERSOONS_sample')
          )
          )

    # For consistency in the joins
    if lazy:
        df_sample = pl.LazyFrame(df_sample)
    else:
        df_sample = pl.DataFrame(df_sample)

    # Prepare the aggregation dataframe
    df_agg = _prepare_dataframe(df_agg, lazy)


    # For each contaxt
    for context in contexts[1:-1]:
        # Read the edgelist
        path_context, set_types = format_path(context, year, cbsdata_path)
        df_context = _load_context_data(path_context, set_types, lazy)
        
        # Merge with previous data
        df = _merge_context_data(df, df_context) # type: ignore
        # Print statistics (only available if not lazy)
        if not lazy:
            logger.info('Context %s added. New data size: %s', context, df.shape) # type: ignore

    # Finally, aggregate data and join with the original sample
    df = _aggregate_data(df, df_agg, contexts[-1], agg_func)  # type: ignore
    df = df_sample.join(df, on=['RINPERSOON', 'RINPERSOONS'], how='left') # type: ignore

    if isinstance(df, pl.LazyFrame):
        df = df.collect()

    if return_pandas:
        return df.to_pandas()
    
    return df

def _validate_columns(df: Union[pl.DataFrame, pl.LazyFrame, pd.DataFrame], columns: list):
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

def _prepare_dataframe(df: Union[pl.DataFrame, pl.LazyFrame, pd.DataFrame], lazy: bool) -> Union[pl.DataFrame, pl.LazyFrame]:
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

    if lazy:
        return pl.LazyFrame(df)
    else:
        return df
    

def _load_context_data(path: str, set_types: Set[int], lazy: bool) -> Union[pl.DataFrame, pl.LazyFrame]:
    """
    Load the context data from the specified path.

    Args:
        path (str): The path to the context data.
        set_types (Set[int]): The set of types to filter.
        lazy (bool): Whether to use lazy evaluation.

    Returns:
        pl.DataFrame: The loaded context data.
    """
    if lazy:
        df_context: Union[pl.DataFrame, pl.LazyFrame] = pl.scan_csv(path)
    else:
        df_context = pl.read_csv(path)
    
    return (df_context
            .filter(pl.col('RELATIE').is_in(set_types))
            .select(['RINPERSOON', 'RINPERSOONS', 'RINPERSOONRELATIE', 'RINPERSOONSRELATIE'])
            )

def _merge_context_data(df: DF, df_context: DF) -> DF:
    """
    Merge the context data with the main dataframe.

    Args:
        df (pl.DataFrame): The main dataframe.
        df_context (pl.DataFrame): The context dataframe.

    Returns:
        pl.DataFrame: The merged dataframe.
    """
    return (df.join(df_context, on=['RINPERSOON', 'RINPERSOONS'], how='inner')
            .select(['RINPERSOON_sample', 'RINPERSOONS_sample', 'RINPERSOONRELATIE', 'RINPERSOONSRELATIE'])
            .rename({'RINPERSOONRELATIE': 'RINPERSOON', 'RINPERSOONSRELATIE': 'RINPERSOONS'})
            )

def _format_vars_agg(agg_cols: str) -> List[str]:
    """
    Format the aggregation columns.

    Args:
        agg_cols (str): The aggregation columns.

    Returns:
        List[str]: The formatted aggregation columns.
    """
    agg_cols = agg_cols.strip().replace(']', '').replace('[', '')
    var_aggs = [_.strip() for _ in agg_cols.split(',')]

    return var_aggs

def _aggregate_data(df: DF, df_agg: DF, agg_cols: str, agg_funcs: List[Callable]) -> DF:
    """
    Aggregate the data based on the specified column and function.

    Args:
        df (pl.DataFrame): The main dataframe.
        df_agg (pl.DataFrame): The aggregation dataframe.
        agg_cols (str): The column to aggregate.
        agg_func (List[Callable]): The aggregation functions.

    Returns:
        pl.DataFrame: The aggregated dataframe.
    """

    var_aggs = _format_vars_agg(agg_cols)

    return (df.join(df_agg, on=['RINPERSOON', 'RINPERSOONS'], how='inner')
            .groupby(['RINPERSOON_sample', 'RINPERSOONS_sample'])
            .agg([agg_func(agg_col).alias(f"{agg_func.__name__}_{agg_col}") for agg_func in agg_funcs for agg_col in var_aggs])
            .rename({'RINPERSOON_sample': 'RINPERSOON', 'RINPERSOONS_sample': 'RINPERSOONS'})
            )
    
