import itertools
import uuid

import coiled
import dask_expr as dd
from dask.distributed import LocalCluster
from prefect import flow, task

from .settings import LOCAL, PROCESSED_DATA_DIR, REDUCED_DATA_DIR, coiled_options, fs


@task
def save_query(region, part_type):

    if LOCAL:
        cluster = LocalCluster()
    else:
        kwargs = {k: v for k, v in coiled_options.items() if k != "local"}
        cluster = coiled.Cluster(**kwargs)

    client = cluster.get_client()  # noqa: F841
    size = 15
    region_ds = dd.read_parquet(PROCESSED_DATA_DIR / "region")
    nation_filtered = dd.read_parquet(PROCESSED_DATA_DIR / "nation")
    supplier_filtered = dd.read_parquet(PROCESSED_DATA_DIR / "supplier")
    part_filtered = dd.read_parquet(PROCESSED_DATA_DIR / "part")
    partsupp_filtered = dd.read_parquet(PROCESSED_DATA_DIR / "partsupp")

    region_filtered = region_ds[(region_ds["r_name"] == region.upper())]
    r_n_merged = nation_filtered.merge(
        region_filtered, left_on="n_regionkey", right_on="r_regionkey", how="inner"
    )
    s_r_n_merged = r_n_merged.merge(
        supplier_filtered,
        left_on="n_nationkey",
        right_on="s_nationkey",
        how="inner",
    )
    ps_s_r_n_merged = s_r_n_merged.merge(
        partsupp_filtered, left_on="s_suppkey", right_on="ps_suppkey", how="inner"
    )
    part_filtered = part_filtered[
        (part_filtered["p_size"] == size)
        & (part_filtered["p_type"].astype(str).str.endswith(part_type.upper()))
    ]
    merged_df = part_filtered.merge(
        ps_s_r_n_merged, left_on="p_partkey", right_on="ps_partkey", how="inner"
    )
    min_values = merged_df.groupby("p_partkey")["ps_supplycost"].min().reset_index()
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
                # "n_name",
                # "s_name",
                # "p_partkey",
            ],
            # ascending=[
            #     False,
            #     True,
            #     True,
            #     True,
            # ],
        )
        .head(100, compute=False)
    )

    outdir = REDUCED_DATA_DIR / region / part_type
    fs.makedirs(outdir, exist_ok=True)

    def name(_):
        return f"{uuid.uuid4()}.snappy.parquet"

    result.to_parquet(outdir, compression="snappy", name_function=name)


@flow
def query_reduce():
    regions = ["europe", "africa", "america", "asia", "middle east"]
    part_types = ["copper", "brass", "tin", "nickel", "steel"]
    for region, part_type in itertools.product(regions, part_types):
        save_query(region, part_type)
