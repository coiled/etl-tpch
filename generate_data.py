import glob
import os
import subprocess
import sys
from datetime import timedelta

from prefect import flow, task


@task(log_prints=True)
def fetch_files():
    data_dir = os.path.join("staged-data", "json")
    os.makedirs(data_dir, exist_ok=True)
    subprocess.check_call(
        [
            sys.executable,
            "tpch.py",
            "--scale",
            "1",
            "--compression",
            "ZSTD",
            "--partition-size",
            "10 MiB",
            "--path",
            data_dir,
        ]
    )
    files = glob.glob(data_dir)
    return files


@flow(log_prints=True)
def generate_data():
    files = fetch_files()
    print(f"Saved {files}")


if __name__ == "__main__":

    generate_data.serve(
        name="generate_data",
        interval=timedelta(seconds=30),
    )
