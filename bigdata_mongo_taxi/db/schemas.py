from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, ConfigDict


class TaxiTrip(BaseModel):
    """Schema for raw TLC yellow taxi rows."""

    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    vendor_id: int = Field(..., alias="VendorID")
    pickup_datetime: datetime = Field(..., alias="tpep_pickup_datetime")
    dropoff_datetime: datetime = Field(..., alias="tpep_dropoff_datetime")
    passenger_count: int
    trip_distance: float
    rate_code_id: int | None = Field(default=None, alias="RatecodeID")
    store_and_fwd_flag: str | None = Field(default=None, alias="store_and_fwd_flag")
    pickup_location_id: int = Field(..., alias="PULocationID")
    dropoff_location_id: int = Field(..., alias="DOLocationID")
    fare_amount: float
    extra: float | None = None
    mta_tax: float | None = Field(default=None, alias="mta_tax")
    tip_amount: float | None = None
    tolls_amount: float | None = None
    improvement_surcharge: float | None = Field(
        default=None, alias="improvement_surcharge"
    )
    congestion_surcharge: float | None = Field(
        default=None, alias="congestion_surcharge"
    )
    total_amount: float
    payment_type: int


class CleanTaxiTrip(BaseModel):
    """Schema for cleaned dataset persisted in MongoDB."""

    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    vendor_id: int = Field(..., alias="VendorID")
    pickup_datetime: datetime
    dropoff_datetime: datetime
    passenger_count: int
    trip_distance: float
    pickup_location_id: int
    dropoff_location_id: int
    fare_amount: float
    tip_amount: float
    total_amount: float
    payment_type: int
    payment_type_label: str
    pickup_date: str
    trip_duration_minutes: float
    store_and_fwd_flag: str | None = None
    rate_code_id: int | None = None
    created_at: datetime

    @classmethod
    def from_trip(cls, **data: Any) -> "CleanTaxiTrip":
        return cls(**data)
