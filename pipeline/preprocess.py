import glob
import os
import shutil

import pandas as pd
from filelock import FileLock
from prefect import flow, task

from .files import RAW_JSON_DIR, STAGING_JSON_DIR, STAGING_PARQUET_DIR

# TODO: Couldn't figure out how to limit concurrent flow runs
# in Prefect, so am using a file lock...
lock = FileLock("preprocess.lock")


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
    with lock:
        files = get_json_files()
        parquet_files = convert_to_parquet.map(files)
        archive_json_file.map(files, wait_for=parquet_files)
