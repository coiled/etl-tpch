import dask.dataframe as dd
import plotly.express as px
import streamlit as st

from pipeline.settings import RESULTS_DIR


@st.cache_data
def get_data(region, part_type):
    return dd.read_parquet(
        RESULTS_DIR / region / part_type.upper() / "*.parquet"
    ).compute()


description = """
### Recommended Suppliers
_Some text that explains the business problem being addressed..._

This query finds which supplier should be selected to place an order for a given part in a given region.
"""
st.markdown(description)
regions = list(map(str.title, ["EUROPE", "AFRICA", "AMERICA", "ASIA", "MIDDLE EAST"]))
region = st.selectbox(
    "Region",
    regions,
    index=None,
    placeholder="Please select a region...",
)
part_types = list(map(str.title, ["COPPER", "BRASS", "TIN", "NICKEL", "STEEL"]))
part_type = st.selectbox(
    "Part Type",
    part_types,
    index=None,
    placeholder="Please select a part type...",
)
if region and part_type:
    df = get_data(region, part_type)
    df = df.rename(
        columns={
            "n_name": "Country",
            "s_name": "Supplier",
            "s_acctbal": "Balance",
            "p_partkey": "Part ID",
        }
    )
    maxes = df.groupby("Country").Balance.idxmax()
    data = df.loc[maxes]
    figure = px.choropleth(
        data,
        locationmode="country names",
        locations="Country",
        featureidkey="Supplier",
        color="Balance",
        color_continuous_scale="viridis",
        hover_data=["Country", "Supplier", "Balance"],
    )
    st.plotly_chart(figure, theme="streamlit", use_container_width=True)
    on = st.toggle("Show data")
    if on:
        st.write(
            df[["Country", "Supplier", "Balance", "Part ID"]], use_container_width=True
        )
