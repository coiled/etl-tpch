import functools

import coiled
import dask
import dask_deltatable as ddt
import pandas as pd
from dask.distributed import LocalCluster
from prefect import flow, task

from .settings import (
    LOCAL,
    PROCESSED_DIR,
    REGION,
    RESULTS_DIR,
    fs,
    lock_compact,
    storage_options,
)

dask.config.set({"dataframe.query-planning": True})


@task
def unshipped_orders_by_revenue(segment):

    if LOCAL:
        cluster = LocalCluster
    else:
        cluster = functools.partial(
            coiled.Cluster,
            name="reduce",
            region=REGION,
            n_workers=10,
            tags={"workflow": "etl-tpch"},
            shutdown_on_close=False,
            idle_timeout="1 minute",
            wait_for_workers=True,
        )

    with cluster() as cluster:
        with cluster.get_client():
            lineitem_ds = ddt.read_deltalake(
                str(PROCESSED_DIR / "lineitem"),
                delta_storage_options=storage_options,
            )
            orders_ds = ddt.read_deltalake(
                str(PROCESSED_DIR / "orders"),
                delta_storage_options=storage_options,
            )
            customer_ds = ddt.read_deltalake(
                str(PROCESSED_DIR / "customer"),
                delta_storage_options=storage_options,
            )

            date = pd.Timestamp.now()
            lsel = lineitem_ds.l_ship_time > date
            osel = orders_ds.o_order_time < date
            csel = customer_ds.c_mktsegment == segment.upper()
            flineitem = lineitem_ds[lsel]
            forders = orders_ds[osel]
            fcustomer = customer_ds[csel]
            jn1 = fcustomer.merge(forders, left_on="c_custkey", right_on="o_custkey")
            jn2 = jn1.merge(flineitem, left_on="o_orderkey", right_on="l_orderkey")
            jn2["revenue"] = jn2.l_extendedprice * (1 - jn2.l_discount)
            total = jn2.groupby(["l_orderkey", "o_order_time", "o_shippriority"])[
                "revenue"
            ].sum()
            result = (
                total.reset_index()
                .sort_values(["revenue"], ascending=False)
                .head(50, compute=False)[
                    ["l_orderkey", "revenue", "o_order_time", "o_shippriority"]
                ]
            ).compute()
            outfile = RESULTS_DIR / f"{segment}.snappy.parquet"
            fs.makedirs(outfile.parent, exist_ok=True)
            result.to_parquet(outfile, compression="snappy")


@flow
def query_reduce():
    with lock_compact:
        segments = ["automobile", "building", "furniture", "machinery", "household"]
        for segment in segments:
            unshipped_orders_by_revenue(segment)
