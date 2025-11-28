import polars as pl

from bigdata_mongo_taxi.pipeline.aggregate import (
    compute_daily_metrics,
    compute_payment_breakdown,
    compute_top_zones,
)


def _df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "pickup_date": ["2024-01-01", "2024-01-01", "2024-01-02"],
            "trip_distance": [4.0, 2.0, 3.0],
            "total_amount": [15.0, 12.0, 14.0],
            "tip_amount": [2.0, 1.5, 2.5],
            "pickup_location_id": [10, 20, 10],
            "payment_type_label": ["credit_card", "cash", "credit_card"],
        }
    )


def test_compute_daily_metrics_returns_sorted_days() -> None:
    df = compute_daily_metrics(_df())
    assert df.shape == (2, 6)
    assert df[0, "pickup_date"] == "2024-01-01"
    assert df[0, "total_trips"] == 2


def test_compute_top_zones_limits_results() -> None:
    df = compute_top_zones(_df(), limit=1)
    assert df.shape[0] == 1
    assert df[0, "pickup_location_id"] == 10


def test_payment_breakdown_sums_revenue() -> None:
    df = compute_payment_breakdown(_df())
    credit_row = df.filter(pl.col("payment_type_label") == "credit_card").to_dicts()[0]
    assert credit_row["total_revenue"] == 29.0
