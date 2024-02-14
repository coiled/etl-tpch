import coiled
import dask
import deltalake
import pandas as pd
from prefect import flow, task
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
    deltalake.write_deltalake(
        outfile, df, mode="append", storage_options=storage_options
    )
    print(f"Saved {outfile}")
    return file


@task
def archive_json_file(file):
    outfile = RAW_JSON_DIR / file.relative_to(STAGING_JSON_DIR)
    fs.makedirs(outfile.parent, exist_ok=True)
    fs.mv(str(file), str(outfile))
    print(f"Archived {str(outfile)}")

    return outfile


def list_new_json_files():
    return list(STAGING_JSON_DIR.rglob("*.json"))


@flow(log_prints=True)
def json_to_parquet():
    files = list_new_json_files()
    files = json_file_to_parquet.map(files)
    archive_json_file.map(files)


@task(log_prints=True)
@coiled.function(
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
    t.vacuum(retention_hours=0, enforce_retention_duration=False, dry_run=False)


@task
def list_tables():
    directories = fs.ls(STAGING_PARQUET_DIR)
    return directories


@flow(log_prints=True)
def compact_tables():
    tables = list_tables()
    compact.map(tables)
