import glob
import time

import dask.dataframe as dd
import streamlit as st


@st.cache_data
def get_data():
    files = "reduced-data/*.parquet"
    while not glob.glob(files):
        print("Waiting for files...")
        time.sleep(10)
    return dd.read_parquet(files).compute()


df = get_data()
options = df.l_returnflag.unique()
returnflag = st.multiselect(
    "l_returnflag",
    options,
)
if not returnflag:
    st.error(f"Please select at least one return code {options.tolist()}")
else:
    data = df.loc[df.l_returnflag.isin(returnflag)]
    st.write("### Return Flag", data)
