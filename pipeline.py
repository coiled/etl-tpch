import glob
import itertools
import os
import shutil
import uuid
from datetime import datetime, timedelta

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
    return outfile


@task
def archive_json_file(file):
    path, basename = os.path.split(file)
    outfile = os.path.join(path.replace(STAGING_JSON_DIR, RAW_JSON_DIR), basename)
    os.makedirs(os.path.dirname(outfile), exist_ok=True)
    shutil.move(file, outfile)


@flow(log_prints=True)
def json_to_parquet():
    with lock_format:
        files = get_json_files()
        for file in files:
            parquet_file = convert_to_parquet(file)
            print(f"Saved {parquet_file}")
            archive_json_file(file)


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
def save_query(files):
    lineitem_ds = dd.read_parquet(files)
    # TODO: We loose datetime info roundtripping through JSON.
    # It would be nice if we kept that information.
    lineitem_ds.l_shipdate = dd.to_datetime(lineitem_ds.l_shipdate)
    lineitem_filtered = lineitem_ds[lineitem_ds.l_shipdate <= datetime(1998, 9, 2)]
    lineitem_filtered["sum_qty"] = lineitem_filtered.l_quantity
    lineitem_filtered["sum_base_price"] = lineitem_filtered.l_extendedprice
    lineitem_filtered["avg_qty"] = lineitem_filtered.l_quantity
    lineitem_filtered["avg_price"] = lineitem_filtered.l_extendedprice
    lineitem_filtered["sum_disc_price"] = lineitem_filtered.l_extendedprice * (
        1 - lineitem_filtered.l_discount
    )
    lineitem_filtered["sum_charge"] = (
        lineitem_filtered.l_extendedprice
        * (1 - lineitem_filtered.l_discount)
        * (1 + lineitem_filtered.l_tax)
    )
    lineitem_filtered["avg_disc"] = lineitem_filtered.l_discount
    lineitem_filtered["count_order"] = lineitem_filtered.l_orderkey
    gb = lineitem_filtered.groupby(["l_returnflag", "l_linestatus"])

    total = gb.agg(
        {
            "sum_qty": "sum",
            "sum_base_price": "sum",
            "sum_disc_price": "sum",
            "sum_charge": "sum",
            "avg_qty": "mean",
            "avg_price": "mean",
            "avg_disc": "mean",
            "count_order": "size",
        }
    )

    result = total.reset_index().sort_values(["l_returnflag", "l_linestatus"])
    os.makedirs(REDUCED_DATA_DIR, exist_ok=True)

    def name(_):
        return f"lineitem_{uuid.uuid4()}.snappy.parquet"

    result.to_parquet(REDUCED_DATA_DIR, compression="snappy", name_function=name)


@flow
def query_reduce():
    files = os.path.join(PROCESSED_DATA_DIR, "lineitem", "*.parquet")
    save_query(files)


# ML model training workflow


@task
def train(model):
    df = pd.read_parquet(REDUCED_DATA_DIR)
    X = df[["sum_qty", "avg_disc"]].astype({"sum_qty": float})
    y = df["l_returnflag"].map({"A": 0, "R": 1, "N": 2}).astype("category")
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
