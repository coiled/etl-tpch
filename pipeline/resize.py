import itertools
import operator
import uuid

import coiled
import dask_expr as dd
from filelock import FileLock
from prefect import flow, task

from .settings import (
    PROCESSED_DATA_DIR,
    RAW_PARQUET_DIR,
    STAGING_PARQUET_DIR,
    coiled_options,
    fs,
)

# TODO: Couldn't figure out how to limit concurrent flow runs
# in Prefect, so am using a file lock...
lock = FileLock("resize.lock")


@task
@coiled.function(**coiled_options)
def repartition_table(files, table):
    df = dd.read_parquet(files)
    df = df.repartition(partition_size="128 MiB")
    outdir = PROCESSED_DATA_DIR / table
    fs.makedirs(outdir, exist_ok=True)

    def name(_):
        return f"{table}_{uuid.uuid4()}.snappy.parquet"

    df.to_parquet(outdir, compression="snappy", name_function=name)


@task
def archive_parquet_files(files):
    # Move original staged files to long-term storage
    for file in files:
        outfile = RAW_PARQUET_DIR / file.relative_to(STAGING_PARQUET_DIR)
        fs.makedirs(outfile.parent, exist_ok=True)
        # Need str(...), otherwise, `TypeError: 'S3Path' object is not iterable`
        fs.mv(str(file), str(outfile))
        print(f"Archived {str(outfile)}")


@flow(log_prints=True)
def resize_parquet():
    """Repartition small Parquet files for future analysis"""
    with lock:
        files = list(STAGING_PARQUET_DIR.rglob("*.parquet"))
        for table, group in itertools.groupby(
            files, key=operator.attrgetter("parent.stem")
        ):
            files_ = list(group)
            futures = repartition_table.submit(files_, table=table)
            archive_parquet_files.submit(files_, wait_for=futures)
