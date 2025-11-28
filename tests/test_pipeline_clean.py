from datetime import datetime, timezone

from bigdata_mongo_taxi.pipeline.clean_transform import tidy_record


def _raw_doc() -> dict:
    return {
        "VendorID": 2,
        "tpep_pickup_datetime": "2024-01-05T12:00:00",
        "tpep_dropoff_datetime": "2024-01-05T12:22:00",
        "passenger_count": None,
        "trip_distance": 4.2,
        "PULocationID": 161,
        "DOLocationID": 90,
        "fare_amount": 18.5,
        "tip_amount": None,
        "total_amount": 22.0,
        "payment_type": 2,
        "store_and_fwd_flag": " y ",
    }


def test_tidy_record_handles_missing_values() -> None:
    clean = tidy_record(_raw_doc())
    assert clean is not None
    assert clean.passenger_count == 1
    assert clean.tip_amount == 0.0


def test_tidy_record_standardizes_datetimes() -> None:
    clean = tidy_record(_raw_doc())
    assert clean is not None
    assert clean.pickup_datetime.tzinfo == timezone.utc
    assert clean.pickup_date == "2024-01-05"
    assert clean.payment_type_label == "cash"
