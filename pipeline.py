import glob
import itertools
import os
import shutil
import uuid
from datetime import timedelta

import dask_expr as dd
import pandas as pd
import requests
import xgboost as xgb
from filelock import FileLock
from prefect import flow, serve, task

STAGING_JSON_DIR = os.path.join("staged-data", "json")
STAGING_PARQUET_DIR = os.path.join("staged-data", "parquet")
RAW_JSON_DIR = os.path.join("raw-data", "json")
RAW_PARQUET_DIR = os.path.join("raw-data", "parquet")
PROCESSED_DATA_DIR = "processed-data"
REDUCED_DATA_DIR = "reduced-data"
MODEL_FILE = "model.json"

# TODO: Couldn't figure out how to limit concurrent flow runs
# in Prefect, so am using a file lock...
lock_format = FileLock("format.lock")
lock_resize = FileLock("resize.lock")


@task(log_prints=True)
def get_json_files():
    """Get paths for new JSON files to convert to Parquet."""
    files = glob.glob(os.path.join(STAGING_JSON_DIR, "**", "*.json"), recursive=True)
    print(f"New JSON files to process: {files}")
    return files


@task(log_prints=True)
def convert_to_parquet(file):
    """Convert raw JSON data file to Parquet."""
    df = pd.read_json(file, compression="zstd")
    path, basename = os.path.split(file)
    outfile = os.path.join(
        path.replace(STAGING_JSON_DIR, STAGING_PARQUET_DIR),
        f"{basename.split('.')[0]}.snappy.parquet",
    )
    os.makedirs(os.path.dirname(outfile), exist_ok=True)
    df.to_parquet(outfile, compression="snappy")
    print(f"Saved {outfile}")
    return outfile


@task
def archive_json_file(file):
    path, basename = os.path.split(file)
    outfile = os.path.join(path.replace(STAGING_JSON_DIR, RAW_JSON_DIR), basename)
    os.makedirs(os.path.dirname(outfile), exist_ok=True)
    shutil.move(file, outfile)
    print(f"Archived {outfile}")


@flow(log_prints=True)
def json_to_parquet():
    with lock_format:
        files = get_json_files()
        parquet_files = convert_to_parquet.map(files)
        archive_json_file.map(files, wait_for=parquet_files)


@task
def repartition_table(table, files):
    df = dd.read_parquet(files)
    df = df.repartition(partition_size="128 MiB")
    outdir = os.path.join(PROCESSED_DATA_DIR, table)
    os.makedirs(outdir, exist_ok=True)

    def name(_):
        return f"{table}_{uuid.uuid4()}.snappy.parquet"

    df.to_parquet(outdir, compression="snappy", name_function=name)


@task
def archive_parquet_files(files):
    # Move original staged files to long-term storage
    for file in files:
        path, basename = os.path.split(file)
        outfile = os.path.join(
            path.replace(STAGING_PARQUET_DIR, RAW_PARQUET_DIR), basename
        )
        os.makedirs(os.path.dirname(outfile), exist_ok=True)
        shutil.move(file, outfile)


@flow(log_prints=True)
def resize_parquet():
    """Repartition small Parquet files for future analysis"""
    with lock_resize:
        files = glob.glob(
            os.path.join(STAGING_PARQUET_DIR, "**", "*.parquet"), recursive=True
        )
        for outdir, group in itertools.groupby(files, key=os.path.dirname):
            files = list(group)
            repartition_table(table=os.path.basename(outdir), files=files)
            archive_parquet_files(files)


@task
def save_query(region, part_type):
    dataset_path = PROCESSED_DATA_DIR + "/"
    size = 15
    region_ds = dd.read_parquet(dataset_path + "region")
    nation_filtered = dd.read_parquet(dataset_path + "nation")
    supplier_filtered = dd.read_parquet(dataset_path + "supplier")
    part_filtered = dd.read_parquet(dataset_path + "part")
    partsupp_filtered = dd.read_parquet(dataset_path + "partsupp")

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

    outdir = os.path.join(REDUCED_DATA_DIR, region, part_type)
    os.makedirs(outdir, exist_ok=True)

    def name(_):
        return f"query_2_{uuid.uuid4()}.snappy.parquet"

    result.to_parquet(outdir, compression="snappy", name_function=name)


@flow
def query_reduce():
    regions = ["europe", "africa", "america", "asia", "middle east"]
    part_types = ["copper", "brass", "tin", "nickel", "steel"]
    for region, part_type in itertools.product(regions, part_types):
        save_query(region, part_type)


# ML model training workflow


@task
def train(model):
    df = pd.read_parquet(os.path.join(REDUCED_DATA_DIR, "europe", "brass"))
    X = df[["p_partkey", "s_acctbal"]]
    y = df["n_name"].map(
        {"FRANCE": 0, "UNITED KINGDOM": 1, "RUSSIA": 2, "GERMANY": 3, "ROMANIA": 4}
    )
    model.fit(X, y)
    return model


@flow(log_prints=True)
def update_model():
    model = xgb.XGBClassifier()
    if os.path.exists(MODEL_FILE):
        model.load_model(MODEL_FILE)
    model = train(model)
    model.save_model(MODEL_FILE)
    print(f"Updated model at {MODEL_FILE}")


# Check health of served ML model


@flow
def check_model_endpoint():
    r = requests.get("http://0.0.0.0:8080/health")
    if not r.json() == ["ok"]:
        raise ValueError("Model endpoint isn't healthy")


if __name__ == "__main__":

    file_conversion = json_to_parquet.to_deployment(
        name="json_to_parquet",
        interval=timedelta(seconds=30),
    )
    data_resize = resize_parquet.to_deployment(
        name="resize_parquet",
        interval=timedelta(seconds=30),
    )
    data_reduction = query_reduce.to_deployment(
        name="reduce_and_save",
        interval=timedelta(minutes=1),
    )
    model_deployment = update_model.to_deployment(
        name="train_model",
        interval=timedelta(minutes=2),
    )
    health_deployment = check_model_endpoint.to_deployment(
        name="model_health",
        interval=timedelta(seconds=10),
    )

    serve(
        file_conversion,
        data_resize,
        data_reduction,
        model_deployment,
        health_deployment,
    )
