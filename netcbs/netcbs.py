"""Package to create network variables (e.g. mean income of family members) from CBS network data (POPNET)."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, List, Set, Tuple, Union, Sequence, cast

import duckdb
import pandas as pd
import pyarrow as pa
import polars as pl

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

codebook: Dict[int, str] = {
    101: "Neighbor - 10 closest addresses",
    102: "Neighborhood acquaintance - 20 random neighbors within 200 meters",
    201: "Colleague",
    301: "Parent",
    302: "Co-parent",
    303: "Grandparent",
    304: "Child",
    305: "Grandchild",
    306: "Full sibling",
    307: "Half sibling",
    308: "Unknown sibling",
    309: "Cousin",
    310: "Nephew/Niece",
    311: "Aunt/Uncle",
    312: "Partner - married",
    313: "Partner - not married",
    314: "Parent-in-law",
    315: "Child-in-law",
    316: "Sibling-in-law",
    317: "Stepparent",
    318: "Stepchild",
    319: "Stepsibling",
    320: "Married full cousin",
    321: "Married cousin",
    322: "Married aunt/uncle",
    401: "Housemate",
    402: "Housemate - institution",
    501: "Classmate primary education",
    502: "Classmate special education",
    503: "Classmate secondary education",
    504: "Classmate vocational education",
    505: "Classmate higher professional education",
    506: "Classmate university education",
}

context2path: Dict[str, str] = {
    "Family": "FAMILIENETWERKTAB",
    "Colleagues": "COLLEGANETWERKTAB",
    "Neighbors": "BURENNETWERKTAB",
    "Schoolmates": "KLASGENOTENNETWERKTAB",
    "Housemates": "HUISGENOTENNETWERKTAB",
}

context2types: Dict[str, Set[int]] = {
    "Family": set(range(301, 323)),
    "Colleagues": {201},
    "Neighbors": {101, 102},
    "Schoolmates": {501, 502, 503, 504, 505, 506},
    "Housemates": {401, 402},
}


def _validate_columns(
    df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame], columns: Sequence[str]
) -> None:
    """
    Validate that a (Pandas/Polars) frame contains required columns.

    Args:
        df: Pandas DataFrame, Polars DataFrame, or Polars LazyFrame.
        columns: Column names that must be present.

    Raises:
        ValueError: If one or more columns are missing.
    """
    # Polars LazyFrame has .columns but may require schema resolution in some cases;
    # for typical usage it is fine.
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in DataFrame: {missing}")


def _format_vars_agg(agg: str) -> List[str]:
    """
    Parse the aggregation variable list from the query tail.

    Expected input format:
        "[var1, var2, ...]"

    Args:
        agg: String segment from the query representing aggregation variables.

    Returns:
        List of cleaned variable names (empty strings removed).
    """
    inner = agg.strip().lstrip("[").rstrip("]")
    return [c.strip() for c in inner.split(",") if c.strip()]


def _parse_context(context: str) -> tuple[str, set[int] | None]:
    """
    Parse "<Base>[all]" or "<Base>[id1,id2,...]".
    Returns (base, ids) where ids=None means "all".
    No validation here.
    """
    base, types = context.split("[", 1)
    base = base.strip()
    types = types.rstrip("]").strip()

    if types.lower() == "all":
        return base, None

    ids = {int(x) for x in types.split(",") if x.strip()}
    return base, ids


def _check_context(context: str) -> tuple[str, set[int]]:
    """
    Strict validator used by validate_query().
    """
    base, ids = _parse_context(context)

    if base not in context2types:
        raise ValueError(f"Unknown context base: {base}")

    allowed = context2types[base]

    if ids is None:
        return base, allowed

    if not ids.issubset(allowed):
        raise ValueError(f"Invalid types {ids} for context {base}")

    return base, ids



def _check_last_version(
    path: Union[str, Path], year: str, format_file: str = "csv"
) -> int:
    """
    Find the highest version number present for a given CBS table and year.

    CBS files are expected to include a 'V<version>' suffix in their filename stem.

    Args:
        path: Folder containing the CBS files.
        year: Year string to match within the filename stem.
        format_file: File extension to search for ("csv" or "parquet").

    Returns:
        The maximum version number found.

    Raises:
        ValueError: If no matching files are found.
    """
    p = Path(path)
    versions = [
        int(f.stem.split("V")[-1]) for f in p.glob(f"*.{format_file}") if year in f.stem
    ]
    if not versions:
        raise ValueError(f"No versions in {path}")
    return max(versions)


def format_path(
    context: str,
    year: int,
    cbsdata_path: str = "G:/Bevolking",
    format_file: str = "parquet",
) -> Tuple[str, Set[int]]:
    """
    Resolve the CBS file path for a given context and year, and return allowed relationship ids.

    This picks the *latest available* version for the table-year combination.

    Args:
        context: Context hop, e.g. "Family[all]" or "Neighbors[101,102]".
        year: Data year.
        cbsdata_path: Base CBS data folder.
        format_file: "csv" or "parquet". For parquet, the function looks under
            a "/geconverteerde data" subfolder.

    Returns:
        (full_path, relationship_ids)

    Raises:
        ValueError: If no versions are found for the requested table/year.
        KeyError: If the context base name is unknown.
    """
    base, ids = _check_context(context)
    if base not in context2types:
        raise ValueError(f"Unknown context base: {base}")

    if ids is None:
        ids = context2types[base]
        
    extra_folder = "" if format_file == "csv" else "/geconverteerde data"
    folder = context2path[base]

    version = _check_last_version(
        f"{cbsdata_path}/{folder}{extra_folder}",
        year=str(year),
        format_file=format_file,
    )
    fname = f"{folder[:-3]}{year}TABV{version}.{format_file}"
    full = f"{cbsdata_path}/{folder}{extra_folder}/{fname}"
    return full, ids


def _to_arrow_table(df: Union[pd.DataFrame, pl.DataFrame]) -> pa.Table:
    """
    Convert a Pandas or Polars DataFrame to a PyArrow Table.

    Used for efficient DuckDB registration (often zero-copy).

    Args:
        df: Pandas DataFrame or Polars DataFrame.

    Returns:
        A PyArrow Table.

    Raises:
        TypeError: If df is not a supported dataframe type.
    """
    if isinstance(df, pl.DataFrame):
        return df.to_arrow()
    if isinstance(df, pd.DataFrame):
        return pa.Table.from_pandas(df, preserve_index=False)
    raise TypeError("df must be a pandas or polars DataFrame")


def validate_query(
    query: str,
    df_sample: Union[pl.DataFrame, pl.LazyFrame, pd.DataFrame],
    df_agg: Union[pl.DataFrame, pl.LazyFrame, pd.DataFrame],
    year: int = 2020,
    cbsdata_path: str = "G:/Bevolking",
    format_file: str = "parquet",
    check_files: bool = True,
) -> List[str]:
    """
    Validate the query and return the parsed contexts (in the order used by `transform`).

    The library expects a query of the form:

        "[agg_var1, agg_var2, ...] -> ContextA[...] -> ContextB[...] -> sample"

    Internally, both `validate_query` and `transform` reverse the segments:
        contexts = query.strip().split(' -> ')[::-1]

    So the returned list is:
        ["sample", "ContextB[...]", "ContextA[...]", "[agg_var1, agg_var2, ...]"]

    Args:
        query: Query string describing the network hops and aggregation variables.
        df_sample: Sample dataframe containing at least 'RINPERSOON'.
        df_agg: Aggregation dataframe containing 'RINPERSOON' and all requested agg vars.
        year: CBS data year for resolving file paths.
        cbsdata_path: Base folder with CBS network tables.
        format_file: "csv" or "parquet" (parquet expects a "geconverteerde data" folder).
        check_files: If True, resolve and validate that the referenced CBS files exist
            (and that a version for that year is available).

    Returns:
        Parsed contexts list in the same order used by `transform()`.

    Raises:
        ValueError: For malformed queries, missing columns, unknown contexts, or invalid types.
    """
    if not isinstance(query, str) or not query.strip():
        raise ValueError("query must be a non-empty string")

    if format_file not in ("csv", "parquet"):
        raise ValueError("format_file must be csv or parquet")

    # Basic query sanity: avoid empty segments like "A ->  -> B"
    parts = [p.strip() for p in query.strip().split("->")]
    if any(not p for p in parts):
        raise ValueError("Query contains an empty segment; use ' -> ' between segments")

    contexts = [p.strip() for p in query.strip().split(" -> ")][::-1]

    if len(contexts) <= 2:
        raise ValueError(
            "Query must contain at least one link (needs: aggvars -> context -> sample)"
        )

    nhops = len(contexts) - 2
    if nhops > 4:
        logger.warning("Query has >4 hops; may be slow or very memory intensive")

    _validate_columns(df_sample, ["RINPERSOON"])

    var_aggs = _format_vars_agg(contexts[-1])
    if not var_aggs:
        raise ValueError(
            "No aggregation variables found; expected something like '[VAR]' at the end of the query"
        )

    _validate_columns(df_agg, ["RINPERSOON", *var_aggs])

    for ctx in contexts[1:-1]:
        base, _ = _check_context(ctx)
        if base not in context2path:
            raise ValueError(
                f"Unknown context base '{base}'. Known: {sorted(context2path.keys())}"
            )

        if check_files:
            _ = format_path(
                ctx, year, cbsdata_path=cbsdata_path, format_file=format_file
            )

    return contexts


def transform(
    query: str,
    df_sample: Union[pd.DataFrame, pl.DataFrame],
    df_agg: Union[pd.DataFrame, pl.DataFrame],
    year: int = 2020,
    agg_funcs: Sequence[str] = ("avg",),
    return_pandas: bool = False,
    cbsdata_path: str = "G:/Bevolking",
    format_file: str = "parquet",
    format_path_fn=None, 
) -> Union[pd.DataFrame, pl.DataFrame]:
    """
    Compute network-based aggregate variables for each person in `df_sample`.

    The query defines:
      1) which CBS network(s) to traverse (1+ hops), and
      2) which variables in `df_agg` to aggregate over the reached alters.

    For each hop, the function reads the relevant CBS network table for `year`,
    filters relationship codes according to the context spec, and performs a join
    to move from the current set of persons to their related persons.

    Finally it aggregates variables from `df_agg` (e.g. income) over the final reached
    persons, grouped by the original sample person.

    Args:
        query: Query string, e.g. "[INCOME] -> Family[all] -> sample".
        df_sample: Sample dataframe with 'RINPERSOON' (one row per ego).
        df_agg: Dataframe with 'RINPERSOON' and the variables referenced in the query.
        year: CBS data year to resolve the network tables.
        agg_funcs: DuckDB aggregation functions to apply (e.g. ('avg','sum')).
        return_pandas: If True, return a pandas.DataFrame; otherwise a polars.DataFrame.
        cbsdata_path: Base CBS data folder.
        format_file: "csv" or "parquet".
        format_path_fn: Optional custom function to resolve CBS file paths for each context.

    Returns:
        A dataframe with the original sample columns plus aggregated variables
        (one row per sample person).

    Raises:
        ValueError: If the query is malformed or required columns are missing.
    """
    if format_path_fn is None:
        format_path_fn = format_path
    
    contexts = query.strip().split(" -> ")[::-1]
    if len(contexts) <= 2:
        raise ValueError("Query must contain at least one link")
    if len(contexts) > 4:
        logger.warning("Query has >4 hops; may be slow")

    if format_file not in ("csv", "parquet"):
        raise ValueError("format_file must be csv or parquet")
    if format_file == "parquet":
        logger.warning(
            'Parquet is faster but relies on "geconverteerde data" and may not exist for all tables.'
        )

    var_aggs = _format_vars_agg(contexts[-1])
    _validate_columns(df_sample, ["RINPERSOON"])
    _validate_columns(df_agg, ["RINPERSOON", *var_aggs])

    # avoid mutating user inputs in-place
    if isinstance(df_sample, pd.DataFrame):
        df_sample2 = df_sample.assign(RINPERSOONsample=df_sample["RINPERSOON"])
    else:
        df_sample2 = df_sample.with_columns(
            pl.col("RINPERSOON").alias("RINPERSOONsample")
        )

    with duckdb.connect(database=":memory:") as con:
        con.register("sample", _to_arrow_table(df_sample2))
        con.register("agg", _to_arrow_table(df_agg))

        last_tbl = "sample"

        # Hop joins with inlined file scan (no ctx{i} materialization)
        for i, ctx in enumerate(contexts[1:-1]):
            logger.info("Processing context %s (%d/%d)", ctx, i + 1, len(contexts) - 2)

            path, types = format_path_fn(ctx, year, cbsdata_path, format_file=format_file)
            tbl_step = f"step{i}"

            type_list = ",".join(map(str, types))
            read_from = (
                f"read_csv_auto('{path}', delim=';')"
                if format_file == "csv"
                else f"read_parquet('{path}')"
            )

            con.execute(f"""
                CREATE OR REPLACE TEMP VIEW {tbl_step} AS
                SELECT prev.RINPERSOONsample,
                       next.RINPERSOONRELATIE AS RINPERSOON
                FROM {last_tbl} AS prev
                JOIN (
                    SELECT RINPERSOON,
                           RINPERSOONRELATIE
                    FROM {read_from}
                    WHERE RELATIE IN ({type_list})
                ) AS next
                  ON prev.RINPERSOON = next.RINPERSOON
            """)
            last_tbl = tbl_step

        logger.info("Processing final aggregation")

        aggs = ", ".join(
            f"{func}({var}) AS {func}_{var}" for func in agg_funcs for var in var_aggs
        )

        con.execute(f"""
            CREATE OR REPLACE TEMP VIEW result AS
            SELECT {last_tbl}.RINPERSOONsample AS RINPERSOON,
                   {aggs}
            FROM {last_tbl}
            JOIN agg
              ON agg.RINPERSOON = {last_tbl}.RINPERSOON
            GROUP BY {last_tbl}.RINPERSOONsample
        """)

        arrow_res = con.execute("""
            SELECT 
                sample.*, 
                result.* EXCLUDE (RINPERSOON)
            FROM sample
            LEFT JOIN result
              ON result.RINPERSOON = sample.RINPERSOON
        """).fetch_arrow_table()

    df_res = cast(pl.DataFrame, pl.from_arrow(arrow_res))
    df_res = df_res.select(pl.all().exclude(["RINPERSOONsample", "RINPERSOON_1"])) 

    return df_res.to_pandas() if return_pandas else df_res
