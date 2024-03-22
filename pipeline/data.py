import datetime
import os
import uuid

import coiled
import duckdb
import numpy as np
import pandas as pd
import psutil
from dask.distributed import print
from prefect import flow, task

from .settings import (
    LOCAL,
    PROCESSED_DIR,
    REGION,
    STAGING_DIR,
    WORKSPACE,
    fs,
    lock_generate,
)


def new_time(t, t_start=None, t_end=None):
    d = pd.Timestamp("1998-12-31") - pd.Timestamp("1992-01-01")
    return t_start + (t - pd.Timestamp("1992-01-01")) * ((t_end - t_start) / d)


@task(log_prints=True)
@coiled.function(
    name="data-generation",
    local=LOCAL,
    region=REGION,
    vm_type="m6i.2xlarge",
    account=WORKSPACE,
)
def generate(scale: float, path: os.PathLike) -> None:
    static_tables = ["customer", "nation", "part", "partsupp", "region", "supplier"]
    with duckdb.connect() as con:
        con.install_extension("tpch")
        con.load_extension("tpch")
        con.sql(
            f"""
            SET memory_limit='{psutil.virtual_memory().available // 2**30 }G';
            SET preserve_insertion_order=false;
            SET threads TO 1;
            SET enable_progress_bar=false;
            """
        )

        print("Generating TPC-H data")
        query = f"call dbgen(sf={scale})"
        con.sql(query)
        print("Finished generating data, exporting...")

        tables = (
            con.sql("select * from information_schema.tables")
            .arrow()
            .column("table_name")
        )
        now = pd.Timestamp.now()
        for table in reversed(sorted(map(str, tables))):
            if table in static_tables and (
                list((STAGING_DIR / table).rglob("*.json"))
                or list((PROCESSED_DIR / table).rglob("*.parquet"))
            ):
                print(f"Static table {table} already exists")
                continue
            print(f"Exporting table: {table}")
            stmt = f"""select * from {table}"""
            df = con.sql(stmt).df()

            # Make order IDs unique across multiple data generation cycles
            if table == "orders":
                # Generate new, random uuid order IDs
                df["o_orderkey_new"] = pd.Series(
                    (uuid.uuid4().hex for _ in range(df.shape[0])),
                    dtype="string[pyarrow]",
                )
                orderkey_new = df[["o_orderkey", "o_orderkey_new"]].set_index(
                    "o_orderkey"
                )
                df = df.drop(columns="o_orderkey").rename(
                    columns={"o_orderkey_new": "o_orderkey"}
                )
            elif table == "lineitem":
                # Join with `orderkey_new` mapping to convert old order IDs to new order IDs
                df = (
                    df.set_index("l_orderkey")
                    .join(orderkey_new)
                    .reset_index(drop=True)
                    .rename(columns={"o_orderkey_new": "l_orderkey"})
                )

            # Shift times to be more recent and lineitem prices to be non-uniform
            if table == "lineitem":
                df["l_shipdate"] = new_time(
                    df["l_shipdate"], t_start=now, t_end=now + pd.Timedelta("3 days")
                )
                df = df.rename(columns={"l_shipdate": "l_ship_time"})
                df["l_extendedprice"] = (
                    np.random.rand(df.shape[0]) * df["l_extendedprice"]
                )
            cols = [c for c in df.columns if "date" in c]
            df[cols] = new_time(
                df[cols], t_start=now - pd.Timedelta("15 minutes"), t_end=now
            )
            df = df.rename(columns={c: c.replace("date", "_time") for c in cols})

            outfile = (
                path
                / table
                / f"{table}_{datetime.datetime.now().isoformat().split('.')[0]}.json"
            )
            fs.makedirs(outfile.parent, exist_ok=True)
            df.to_json(
                outfile,
                date_format="iso",
                orient="records",
                lines=True,
            )
            print(f"Exported table {table} to {outfile}")
        print("Finished exporting all data")


@flow
def generate_data():
    with lock_generate:
        generate(
            scale=1,
            path=STAGING_DIR,
        )
