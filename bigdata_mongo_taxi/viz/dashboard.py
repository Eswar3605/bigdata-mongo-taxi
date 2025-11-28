from __future__ import annotations

from pathlib import Path
import sys

import polars as pl
import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from bigdata_mongo_taxi.db.mongo_client import get_db


st.set_page_config(page_title="NYC Taxi Gold Metrics", layout="wide")
db = get_db()


@st.cache_data(ttl=300)
def load_collection(name: str) -> pl.DataFrame:
    docs = list(db[name].find({}, {"_id": 0}))
    if not docs:
        return pl.DataFrame()
    return pl.DataFrame(docs)


st.title("NYC Yellow Taxi – MongoDB Gold Layer")

daily_df = load_collection("trips_gold_daily")
zone_df = load_collection("trips_gold_zones")
payment_df = load_collection("trips_gold_payment")

st.subheader("Daily Revenue & Trips")
if daily_df.is_empty():
    st.info("Run the aggregation pipeline to populate gold collections.")
else:
    daily_pd = (
        daily_df.with_columns(
            pl.col("pickup_date")
            .str.strptime(pl.Date, strict=False)
            .alias("pickup_date")
        )
        .sort("pickup_date")
        .to_pandas()
        .set_index("pickup_date")
    )
    st.line_chart(daily_pd[["total_revenue", "total_trips"]])

col1, col2 = st.columns(2)

with col1:
    st.subheader("Top Pickup Zones")
    if zone_df.is_empty():
        st.info("No zone data yet.")
    else:
        st.bar_chart(
            zone_df.sort("total_trips", descending=True)
            .to_pandas()
            .set_index("pickup_location_id")[["total_trips"]]
        )

with col2:
    st.subheader("Payment Breakdown")
    if payment_df.is_empty():
        st.info("No payment metrics yet.")
    else:
        payment_pd = payment_df.to_pandas().set_index("payment_type_label")
        st.bar_chart(payment_pd["total_revenue"])
        st.dataframe(payment_pd)
