import polars as pl
import pytest
from pathlib import Path

from netcbs import transform, validate_query


@pytest.fixture
def df_sample():
    return pl.DataFrame({"RINPERSOON": [1, 2, 3]})


@pytest.fixture
def df_agg():
    return pl.DataFrame(
        {"RINPERSOON": [10, 11, 12, 20, 21, 22], "Income": [1000, 2000, 3000, 4000, 5000, 6000]}
    )


@pytest.fixture
def hop_parquet(tmp_path: Path) -> str:
    df_net = pl.DataFrame(
        {
            "RINPERSOON": [1, 1, 2, 2, 3, 3],
            "RINPERSOONRELATIE": [10, 11, 20, 21, 12, 22],
            "RELATIE": [301, 301, 301, 301, 301, 301],
        }
    )
    path = tmp_path / "hop.parquet"
    df_net.write_parquet(path)
    return str(path)


def test_transform_one_hop_avg_income(df_sample, df_agg, hop_parquet):
    def fake_format_path(context: str, year: int, cbsdata_path: str = "X", format_file: str = "parquet"):
        return hop_parquet, {301}

    out = transform(
        "[Income] -> Family[301] -> sample",
        df_sample,
        df_agg,
        year=2020,
        agg_funcs=("avg",),
        format_file="parquet",
        format_path_fn=fake_format_path,  
    )

    got = {r["RINPERSOON"]: r["avg_Income"] for r in out.select(["RINPERSOON", "avg_Income"]).to_dicts()}
    assert got[1] == pytest.approx(1500.0)
    assert got[2] == pytest.approx(4500.0)
    assert got[3] == pytest.approx(4500.0)


def test_transform_preserves_sample_rows_when_no_links(df_sample, df_agg, hop_parquet):
    def fake_format_path_no_links(context: str, year: int, cbsdata_path: str = "X", format_file: str = "parquet"):
        return hop_parquet, {999}

    out = transform(
        "[Income] -> Family[999] -> sample",
        df_sample,
        df_agg,
        year=2020,
        agg_funcs=("avg",),
        format_file="parquet",
        format_path_fn=fake_format_path_no_links,
    )

    assert out.height == df_sample.height
    assert out["avg_Income"].null_count() == out.height
