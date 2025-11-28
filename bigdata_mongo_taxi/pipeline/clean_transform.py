from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Iterable

from pymongo import InsertOne
from pymongo.collection import Collection

from ..db.mongo_client import get_db
from ..db.schemas import CleanTaxiTrip, TaxiTrip
from ..logging_conf import setup_logging

BATCH_SIZE = 5_000
NUMERIC_DEFAULTS: dict[str, float] = {
    "passenger_count": 1,
    "trip_distance": 0.0,
    "fare_amount": 0.0,
    "tip_amount": 0.0,
    "total_amount": 0.0,
}

PAYMENT_TYPE_LABELS = {
    1: "credit_card",
    2: "cash",
    3: "no_charge",
    4: "dispute",
    5: "unknown",
    6: "voided",
}


def _ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _normalize_text(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.strip()
    return cleaned.upper() if cleaned else None


def _fill_missing(row: dict[str, Any]) -> None:
    for key, default in NUMERIC_DEFAULTS.items():
        value = row.get(key)
        if value in (None, ""):
            row[key] = default
    if not row.get("tip_amount"):
        row["tip_amount"] = 0.0


def _payment_label(payment_type: int) -> str:
    return PAYMENT_TYPE_LABELS.get(payment_type, "other")


def tidy_record(raw_doc: dict[str, Any]) -> CleanTaxiTrip | None:
    working = raw_doc.copy()
    working.pop("_id", None)
    _fill_missing(working)
    working["store_and_fwd_flag"] = _normalize_text(
        working.get("store_and_fwd_flag")
    )

    try:
        trip = TaxiTrip(**working)
    except Exception:
        return None

    pickup_dt = _ensure_utc(trip.pickup_datetime)
    dropoff_dt = _ensure_utc(trip.dropoff_datetime)
    trip_minutes = max((dropoff_dt - pickup_dt).total_seconds() / 60, 0.0)

    clean_trip = CleanTaxiTrip(
        VendorID=trip.vendor_id,
        pickup_datetime=pickup_dt,
        dropoff_datetime=dropoff_dt,
        passenger_count=max(trip.passenger_count, 1),
        trip_distance=max(trip.trip_distance, 0.0),
        pickup_location_id=trip.pickup_location_id,
        dropoff_location_id=trip.dropoff_location_id,
        fare_amount=trip.fare_amount,
        tip_amount=trip.tip_amount or 0.0,
        total_amount=max(trip.total_amount, 0.0),
        payment_type=trip.payment_type,
        payment_type_label=_payment_label(trip.payment_type),
        pickup_date=pickup_dt.date().isoformat(),
        trip_duration_minutes=round(trip_minutes, 2),
        store_and_fwd_flag=trip.store_and_fwd_flag,
        rate_code_id=trip.rate_code_id,
        created_at=datetime.now(timezone.utc),
    )
    return clean_trip


def process_batch(
    raw_records: Iterable[dict[str, Any]],
    clean_collection: Collection,
    dedupe_keys: set[tuple[Any, ...]],
    logger: logging.Logger,
) -> int:
    operations: list[InsertOne] = []
    inserted = 0

    for raw_doc in raw_records:
        clean_trip = tidy_record(raw_doc)
        if clean_trip is None:
            logger.debug("Skipping invalid row: %s", raw_doc)
            continue

        key = (
            clean_trip.vendor_id,
            clean_trip.pickup_datetime,
            clean_trip.dropoff_datetime,
            clean_trip.total_amount,
        )
        if key in dedupe_keys:
            continue
        dedupe_keys.add(key)

        operations.append(InsertOne(clean_trip.model_dump(by_alias=True)))

        if len(operations) >= BATCH_SIZE:
            result = clean_collection.bulk_write(operations, ordered=False)
            inserted += result.inserted_count
            operations.clear()

    if operations:
        result = clean_collection.bulk_write(operations, ordered=False)
        inserted += result.inserted_count

    return inserted


def clean_raw_collection(batch_size: int = BATCH_SIZE) -> None:
    setup_logging()
    logger = logging.getLogger(__name__)
    db = get_db()
    raw_collection = db["trips_raw"]
    clean_collection = db["trips_clean"]
    clean_collection.create_index(
        [
            ("VendorID", 1),
            ("pickup_datetime", 1),
            ("dropoff_datetime", 1),
            ("total_amount", 1),
        ],
        name="dedupe_idx",
        unique=True,
    )

    logger.info("Starting clean pipeline, batch_size=%s", batch_size)
    cursor = raw_collection.find({}, batch_size=batch_size)
    dedupe_cache: set[tuple[Any, ...]] = set()
    batch: list[dict[str, Any]] = []
    total_inserted = 0

    for doc in cursor:
        batch.append(doc)
        if len(batch) >= batch_size:
            total_inserted += process_batch(batch, clean_collection, dedupe_cache, logger)
            batch.clear()

    if batch:
        total_inserted += process_batch(batch, clean_collection, dedupe_cache, logger)

    logger.info("Finished clean pipeline, inserted %s docs", total_inserted)


if __name__ == "__main__":
    clean_raw_collection()
