import datetime
import time

import pandas as pd
import streamlit as st

from pipeline.settings import RESULTS_DIR


@st.cache_data
def get_data(segment):
    return pd.read_parquet(RESULTS_DIR / f"{segment.lower()}.snappy.parquet")


st.markdown(
    """
### Top Unshipped Orders
_Top 50 unshipped orders with the highest revenue._
"""
)

SEGMENTS = ["automobile", "building", "furniture", "machinery", "household"]


def files_exist():
    # Do we have all the files needed for the dashboard?
    files = list(RESULTS_DIR.rglob("*.snappy.parquet"))
    return len(files) == len(SEGMENTS)


with st.spinner("Waiting for data..."):
    while not files_exist():
        time.sleep(5)

segments = list(
    map(str.title, ["automobile", "building", "furniture", "machinery", "household"])
)
segment = st.selectbox(
    "Segment",
    segments,
    index=None,
    placeholder="Please select a product segment...",
)
if segment:
    df = get_data(segment)
    df = df.drop(columns="o_shippriority")
    df["l_orderkey"] = df["l_orderkey"].map(lambda x: f"{x:09}")
    df["revenue"] = df["revenue"].round(2)
    now = datetime.datetime.now()
    dt = now.date() - datetime.date(1995, 3, 15)
    df["o_orderdate"] = (df["o_orderdate"] + dt).dt.date
    df = df.rename(
        columns={
            "l_orderkey": "Order ID",
            "o_orderdate": "Date Ordered",
            "revenue": "Revenue",
        }
    )

    df = df.set_index("Order ID")
    st.dataframe(
        df.style.format({"Revenue": "${:,}"}),
        column_config={
            "Date Ordered": st.column_config.DateColumn(
                "Date Ordered",
                format="MM/DD/YYYY",
                help="Date order was placed",
            ),
            "Revenue": st.column_config.NumberColumn(
                "Revenue (in USD)",
                help="Total revenue of order",
            ),
        },
    )
