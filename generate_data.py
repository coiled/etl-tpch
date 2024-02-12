import datetime
import enum
import pathlib
from datetime import timedelta

import boto3
import botocore.session
import coiled
import duckdb
import psutil
import pyarrow.compute as pc
from prefect import flow, task

from pipeline.files import STAGING_JSON_DIR, fs
from pipeline.settings import LOCAL


@task
@coiled.function(local=LOCAL, region="us-east-1")
def generate(scale: float, path: str) -> str:
    with duckdb.connect() as con:
        con.install_extension("tpch")
        con.load_extension("tpch")

        if str(path).startswith("s3://"):
            path += "/" if not path.endswith("/") else ""
            REGION = get_bucket_region(path)
            session = botocore.session.Session()
            creds = session.get_credentials()
            con.install_extension("httpfs")
            con.load_extension("httpfs")
            con.sql(
                f"""
                SET s3_region='{REGION}';
                SET s3_access_key_id='{creds.access_key}';
                SET s3_secret_access_key='{creds.secret_key}';
                SET s3_session_token='{creds.token}';
                """
            )
        else:
            path = (
                pathlib.Path(path)
            )
            path.mkdir(parents=True, exist_ok=True)

        con.sql(
            f"""
            SET memory_limit='{psutil.virtual_memory().available // 2**30 }G';
            SET preserve_insertion_order=false;
            SET threads TO 1;
            SET enable_progress_bar=false;
            """
        )

        print("Generating TPC-H data")
        query = f"call dbgen(sf={scale})"
        con.sql(query)
        print("Finished generating data, exporting...")

        tables = (
            con.sql("select * from information_schema.tables")
            .arrow()
            .column("table_name")
        )
        for table in map(str, tables):
            print(f"Exporting table: {table}")
            if str(path).startswith("s3://"):
                out = path + table
            else:
                out = path / table

            stmt = f"""select * from {table}"""
            df = con.sql(stmt).arrow()

            file = f"{table}_{datetime.datetime.now().isoformat().split('.')[0]}.json"
            if isinstance(out, str) and out.startswith("s3"):
                out_ = f"{out}/{file}"
            else:
                out_ = pathlib.Path(out)
                out_.mkdir(exist_ok=True, parents=True)
                out_ = str(out_ / file)

            df.to_pandas().to_json(
                out_,
                date_format="iso",
                orient="records",
                lines=True,
            )
            print(f"Exported table {table} to {out_}")
        print("Finished exporting all data")


def get_bucket_region(path: str):
    if not path.startswith("s3://"):
        raise ValueError(f"'{path}' is not an S3 path")
    bucket = path.replace("s3://", "").split("/")[0]
    resp = boto3.client("s3").get_bucket_location(Bucket=bucket)
    return resp["LocationConstraint"] or "us-east-1"


@flow(log_prints=True)
def generate_data(data_dir):
    fs.makedirs(data_dir, exist_ok=True)
    generate(
        scale=0.01,
        path=data_dir,
    )


if __name__ == "__main__":
    generate_data.serve(
        name="generate_data",
        parameters={"data_dir": STAGING_JSON_DIR},
        interval=timedelta(seconds=20),
    )
