import functools
import itertools

import coiled
import dask_deltatable
from dask.distributed import LocalCluster
from prefect import flow, task

from .settings import (
    LOCAL,
    REDUCED_DATA_DIR,
    REGION,
    STAGING_PARQUET_DIR,
    fs,
    lock_compact,
    storage_options,
)


@task
def save_query(region, part_type):

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
            idle_timeout="5 minutes",
            wait_for_workers=True,
        )

    with cluster() as cluster:
        with cluster.get_client():
            size = 15
            region_ds = dask_deltatable.read_deltalake(
                str(STAGING_PARQUET_DIR / "region"),
                delta_storage_options=storage_options,
            )
            nation_filtered = dask_deltatable.read_deltalake(
                str(STAGING_PARQUET_DIR / "nation"),
                delta_storage_options=storage_options,
            )
            supplier_filtered = dask_deltatable.read_deltalake(
                str(STAGING_PARQUET_DIR / "supplier"),
                delta_storage_options=storage_options,
            )
            part_filtered = dask_deltatable.read_deltalake(
                str(STAGING_PARQUET_DIR / "part"), delta_storage_options=storage_options
            )
            partsupp_filtered = dask_deltatable.read_deltalake(
                str(STAGING_PARQUET_DIR / "partsupp"),
                delta_storage_options=storage_options,
            )

            region_filtered = region_ds[(region_ds["r_name"] == region.upper())]
            r_n_merged = nation_filtered.merge(
                region_filtered,
                left_on="n_regionkey",
                right_on="r_regionkey",
                how="inner",
            )
            s_r_n_merged = r_n_merged.merge(
                supplier_filtered,
                left_on="n_nationkey",
                right_on="s_nationkey",
                how="inner",
            )
            ps_s_r_n_merged = s_r_n_merged.merge(
                partsupp_filtered,
                left_on="s_suppkey",
                right_on="ps_suppkey",
                how="inner",
            )
            part_filtered = part_filtered[
                (part_filtered["p_size"] == size)
                & (part_filtered["p_type"].astype(str).str.endswith(part_type.upper()))
            ]
            merged_df = part_filtered.merge(
                ps_s_r_n_merged, left_on="p_partkey", right_on="ps_partkey", how="inner"
            )
            min_values = (
                merged_df.groupby("p_partkey")["ps_supplycost"].min().reset_index()
            )
            min_values.columns = ["P_PARTKEY_CPY", "MIN_SUPPLYCOST"]
            merged_df = merged_df.merge(
                min_values,
                left_on=["p_partkey", "ps_supplycost"],
                right_on=["P_PARTKEY_CPY", "MIN_SUPPLYCOST"],
                how="inner",
            )

            result = (
                merged_df[
                    [
                        "s_acctbal",
                        "s_name",
                        "n_name",
                        "p_partkey",
                        "p_mfgr",
                        "s_address",
                        "s_phone",
                        "s_comment",
                    ]
                ]
                .sort_values(
                    by=[
                        "s_acctbal",
                        "n_name",
                        "s_name",
                        "p_partkey",
                    ],
                    ascending=[
                        False,
                        True,
                        True,
                        True,
                    ],
                )
                .head(100)
            )

            outfile = REDUCED_DATA_DIR / region / part_type / "result.snappy.parquet"
            fs.makedirs(outfile.parent, exist_ok=True)
            result.to_parquet(outfile, compression="snappy")


@flow
def query_reduce():
    with lock_compact:
        regions = ["europe", "africa", "america", "asia", "middle east"]
        part_types = ["copper", "brass", "tin", "nickel", "steel"]
        for region, part_type in itertools.product(regions, part_types):
            save_query(region, part_type)
