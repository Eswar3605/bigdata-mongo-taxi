from __future__ import annotations

import logging
from typing import Any

import polars as pl
from pymongo.collection import Collection

from ..db.mongo_client import get_db
from ..logging_conf import setup_logging


def _collection(name: str) -> Collection:
    return get_db()[name]


def compute_daily_metrics(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return df
    return (
        df.group_by("pickup_date")
        .agg(
            pl.len().alias("total_trips"),
            pl.col("trip_distance").sum().alias("total_distance"),
            pl.col("trip_distance").mean().alias("avg_distance"),
            pl.col("total_amount").sum().alias("total_revenue"),
            pl.col("tip_amount").mean().alias("avg_tip"),
        )
        .sort("pickup_date")
    )


def compute_top_zones(df: pl.DataFrame, limit: int = 10) -> pl.DataFrame:
    if df.is_empty():
        return df
    return (
        df.group_by("pickup_location_id")
        .agg(
            pl.len().alias("total_trips"),
            pl.col("total_amount").sum().alias("total_revenue"),
            pl.col("trip_distance").mean().alias("avg_distance"),
        )
        .sort("total_trips", descending=True)
        .head(limit)
    )


def compute_payment_breakdown(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return df
    return (
        df.group_by("payment_type_label")
        .agg(
            pl.len().alias("total_trips"),
            pl.col("total_amount").sum().alias("total_revenue"),
        )
        .sort("total_trips", descending=True)
    )


def _write_dataframe(df: pl.DataFrame, collection: Collection, index_field: str) -> int:
    if df.is_empty():
        collection.delete_many({})
        return 0

    payload = df.to_dicts()
    collection.delete_many({})
    if payload:
        collection.insert_many(payload)
        collection.create_index(index_field, unique=True)
    return len(payload)


def aggregate_clean_collection() -> None:
    setup_logging()
    logger = logging.getLogger(__name__)

    clean_docs = list(_collection("trips_clean").find({}, {"_id": 0}))
    if not clean_docs:
        logger.warning("No cleaned records found; skipping aggregation.")
        return

    clean_df = pl.DataFrame(clean_docs)
    daily_df = compute_daily_metrics(clean_df)
    zone_df = compute_top_zones(clean_df)
    payment_df = compute_payment_breakdown(clean_df)

    daily_count = _write_dataframe(daily_df, _collection("trips_gold_daily"), "pickup_date")
    zone_count = _write_dataframe(zone_df, _collection("trips_gold_zones"), "pickup_location_id")
    payment_count = _write_dataframe(
        payment_df, _collection("trips_gold_payment"), "payment_type_label"
    )

    logger.info(
        "Aggregation complete daily=%s zones=%s payment=%s",
        daily_count,
        zone_count,
        payment_count,
    )


if __name__ == "__main__":
    aggregate_clean_collection()
