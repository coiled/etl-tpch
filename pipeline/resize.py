import glob
import itertools
import os
import shutil
import uuid

import dask_expr as dd
from filelock import FileLock
from prefect import flow, task

from .files import PROCESSED_DATA_DIR, RAW_PARQUET_DIR, STAGING_PARQUET_DIR

# TODO: Couldn't figure out how to limit concurrent flow runs
# in Prefect, so am using a file lock...
lock = FileLock("resize.lock")


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
    with lock:
        files = glob.glob(
            os.path.join(STAGING_PARQUET_DIR, "**", "*.parquet"), recursive=True
        )
        for outdir, group in itertools.groupby(files, key=os.path.dirname):
            files = list(group)
            repartition_table(table=os.path.basename(outdir), files=files)
            archive_parquet_files(files)
