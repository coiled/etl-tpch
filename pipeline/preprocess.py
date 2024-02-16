import coiled
import dask
import deltalake
import pandas as pd
import pyarrow as pa
from dask.distributed import print
from prefect import flow, task
from prefect.concurrency.sync import concurrency
from prefect.tasks import exponential_backoff

from .settings import (
    LOCAL,
    RAW_JSON_DIR,
    REGION,
    STAGING_JSON_DIR,
    STAGING_PARQUET_DIR,
    fs,
    storage_options,
)

dask.config.set({"coiled.use_aws_creds_endpoint": False})


@task(
    log_prints=True,
    retries=10,
    retry_delay_seconds=exponential_backoff(10),
    retry_jitter_factor=1,
)
@coiled.function(
    name="preprocessing",
    local=LOCAL,
    region=REGION,
    keepalive="10 minutes",
    tags={"workflow": "etl-tpch"},
)
def json_file_to_parquet(file):
    """Convert raw JSON data file to Parquet."""
    print(f"Processing {file}")
    df = pd.read_json(file, lines=True)
    outfile = STAGING_PARQUET_DIR / file.parent.name
    fs.makedirs(outfile.parent, exist_ok=True)
    data = pa.Table.from_pandas(df, preserve_index=False)
    deltalake.write_deltalake(
        outfile, data, mode="append", storage_options=storage_options
    )
    print(f"Saved {outfile}")
    return file


@task
def archive_json_file(file):
    outfile = RAW_JSON_DIR / file.relative_to(STAGING_JSON_DIR)
    fs.makedirs(outfile.parent, exist_ok=True)
    fs.mv(str(file), str(outfile))

    return outfile


def list_new_json_files():
    return list(STAGING_JSON_DIR.rglob("*.json"))


@flow(log_prints=True)
def json_to_parquet():
    with concurrency("json_to_parquet", occupy=1):
        files = list_new_json_files()
        files = json_file_to_parquet.map(files)
        futures = archive_json_file.map(files)
        for f in futures:
            print(f"Archived {str(f.result())}")


@task(log_prints=True)
@coiled.function(
    name="preprocessing",
    local=LOCAL,
    region=REGION,
    keepalive="10 minutes",
    tags={"workflow": "etl-tpch"},
)
def compact(table):
    print(f"Compacting table {table}")
    table = table if LOCAL else f"s3://{table}"
    t = deltalake.DeltaTable(table, storage_options=storage_options)
    t.optimize.compact()
    # t.vacuum(retention_hours=0, enforce_retention_duration=False, dry_run=False)
    return table


@task
def list_tables():
    if not fs.exists(STAGING_PARQUET_DIR):
        return []
    directories = fs.ls(STAGING_PARQUET_DIR, refresh=True)
    return directories


@flow(log_prints=True)
def compact_tables():
    with concurrency("compact", occupy=1):
        tables = list_tables()
        futures = compact.map(tables)
        for f in futures:
            print(f"Finished compacting {f.result()} table")
