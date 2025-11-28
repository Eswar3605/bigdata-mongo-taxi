import argparse
import logging
from pathlib import Path

import polars as pl
from pymongo import InsertOne

from ..logging_conf import setup_logging
from ..db.mongo_client import get_db
from ..db.schemas import TaxiTrip

BATCH_SIZE = 10_000


def ingest_csv_to_mongo(csv_path: Path) -> None:
    setup_logging()
    logger = logging.getLogger(__name__)

    if not csv_path.exists():
        raise FileNotFoundError(f"{csv_path} not found")

    db = get_db()
    collection = db["trips_raw"]

    logger.info("Starting ingestion from %s", csv_path)

    if csv_path.suffix.lower() == ".parquet":
        df = pl.read_parquet(csv_path)
    else:
        df = pl.read_csv(csv_path, infer_schema_length=10_000, low_memory=True)
    total_inserted = 0
    for batch_df in df.iter_slices(BATCH_SIZE):
        records: list[InsertOne] = []

        for row in batch_df.to_dicts():
            try:
                trip = TaxiTrip(**row)
                doc = trip.model_dump(by_alias=True)
                records.append(InsertOne(doc))
            except Exception as exc:
                logger.debug("Skipping invalid row: %s", exc)

        if records:
            result = collection.bulk_write(records, ordered=False)
            total_inserted += result.inserted_count

    logger.info("Finished ingestion inserted=%s", total_inserted)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load NYC taxi CSV into MongoDB")
    parser.add_argument(
        "csv_path",
        type=Path,
        help="Path to yellow_tripdata CSV file (≥1 month)",
    )
    args = parser.parse_args()
    ingest_csv_to_mongo(args.csv_path)
