import coiled
import fsspec
import pandas as pd
from filelock import FileLock
from prefect import flow, task

from .files import RAW_JSON_DIR, STAGING_JSON_DIR, STAGING_PARQUET_DIR
from .settings import LOCAL


# TODO: I want to use `get_filesystem` in `pipeline.files`
# but Prefect encounters serialization issues. Defining the function
# here avoids this issue.
def get_filesystem(path):
    protocol = fsspec.utils.get_protocol(path)
    return fsspec.filesystem(protocol)


# TODO: Couldn't figure out how to limit concurrent flow runs
# in Prefect, so am using a file lock...
lock = FileLock("preprocess.lock")


@task(log_prints=True)
@coiled.function(local=LOCAL)
def convert_to_parquet(file):
    """Convert raw JSON data file to Parquet."""
    print(f"Processing {file}")
    df = pd.read_json(file, compression="zstd")
    outfile = STAGING_PARQUET_DIR / file.relative_to(STAGING_JSON_DIR).with_suffix(
        ".snappy.parquet"
    )
    fs = get_filesystem(file)
    fs.makedirs(outfile.parent, exist_ok=True)
    df.to_parquet(outfile, compression="snappy")
    print(f"Saved {outfile}")
    return outfile


@task
def archive_json_file(file):
    outfile = RAW_JSON_DIR / file.relative_to(STAGING_JSON_DIR)
    fs = get_filesystem(file)
    fs.makedirs(outfile.parent, exist_ok=True)
    # Need str(...), otherwise, `TypeError: 'S3Path' object is not iterable`
    fs.mv(str(file), str(outfile))
    print(f"Archived {str(outfile)}")


@flow(log_prints=True)
def json_to_parquet():
    with lock:
        files = list(STAGING_JSON_DIR.rglob("*.json"))
        parquet_files = convert_to_parquet.map(files)
        archive_json_file.map(files, wait_for=parquet_files)
