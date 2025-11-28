from datetime import datetime

import pytest
from pydantic import ValidationError

from bigdata_mongo_taxi.db.schemas import TaxiTrip


def _base_payload() -> dict:
    return {
        "VendorID": 1,
        "tpep_pickup_datetime": "2024-01-01T00:10:00",
        "tpep_dropoff_datetime": "2024-01-01T00:25:00",
        "passenger_count": 2,
        "trip_distance": 3.1,
        "PULocationID": 132,
        "DOLocationID": 45,
        "fare_amount": 12.5,
        "tip_amount": 2.0,
        "total_amount": 16.0,
        "payment_type": 1,
    }


def test_taxi_trip_parses_datetimes() -> None:
    payload = _base_payload()
    trip = TaxiTrip(**payload)
    assert isinstance(trip.pickup_datetime, datetime)
    assert trip.passenger_count == 2


def test_missing_required_field_raises() -> None:
    payload = _base_payload()
    payload.pop("VendorID")
    with pytest.raises(ValidationError):
        TaxiTrip(**payload)
