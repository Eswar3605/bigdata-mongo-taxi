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


def summarize_daily(df: pl.DataFrame) -> tuple[float, int, float]:
    if df.is_empty():
        return 0.0, 0, 0.0
    return (
        float(df["total_revenue"].sum()),
        int(df["total_trips"].sum()),
        float(df["total_distance"].sum()),
    )


st.title("NYC Yellow Taxi – MongoDB Gold Layer")
st.caption("Data source: NYC TLC Yellow Taxi (Jan 2022) | Backed by MongoDB gold collections")

daily_df = load_collection("trips_gold_daily")
zone_df = load_collection("trips_gold_zones")
payment_df = load_collection("trips_gold_payment")

if daily_df.is_empty() or zone_df.is_empty() or payment_df.is_empty():
    st.warning("Gold collections are empty. Run the aggregation pipeline first.")
    st.stop()

daily_df = (
    daily_df.with_columns(
        pl.col("pickup_date").str.strptime(pl.Date, strict=False).alias("pickup_date")
    ).sort("pickup_date")
)

total_revenue, total_trips, total_distance = summarize_daily(daily_df)

metric_col1, metric_col2, metric_col3 = st.columns(3)
metric_col1.metric("Total Revenue", f"${total_revenue:,.0f}")
metric_col2.metric("Total Trips", f"{total_trips:,}")
metric_col3.metric("Total Distance (mi)", f"{total_distance:,.0f}")

st.markdown("---")

start_date, end_date = st.slider(
    "Select date range",
    value=(
        daily_df["pickup_date"].min(),
        daily_df["pickup_date"].max(),
    ),
    format="YYYY-MM-DD",
)

filtered_daily = daily_df.filter(
    (pl.col("pickup_date") >= start_date) & (pl.col("pickup_date") <= end_date)
)

st.subheader("Daily Revenue & Trips")
daily_pd = filtered_daily.to_pandas().set_index("pickup_date")
st.line_chart(daily_pd[["total_revenue", "total_trips"]])

st.markdown("---")

zone_limit = st.slider("Top pickup zones to display", min_value=5, max_value=20, value=10)
col1, col2 = st.columns(2)

with col1:
    st.subheader("Top Pickup Zones")
    zone_pd = (
        zone_df.sort("total_trips", descending=True)
        .head(zone_limit)
        .to_pandas()
        .set_index("pickup_location_id")
    )
    st.bar_chart(zone_pd["total_trips"])
    st.caption("Based on cleaned trips for the selected month.")

with col2:
    st.subheader("Payment Breakdown")
    payment_pd = payment_df.to_pandas().set_index("payment_type_label")
    focus_payment = st.selectbox(
        "Highlight payment type", payment_pd.index.tolist(), index=0
    )
    st.bar_chart(payment_pd["total_revenue"])
    st.dataframe(payment_pd)
    st.info(
        f"{focus_payment} accounts for "
        f"{payment_pd.loc[focus_payment, 'total_trips']:,} trips "
        f"and ${payment_pd.loc[focus_payment, 'total_revenue']:,.0f} revenue."
    )
