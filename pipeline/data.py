import datetime
import os

import botocore.session
import coiled
import duckdb
import psutil
from dask.distributed import print
from prefect import flow, task

from .settings import LOCAL, REGION, STAGING_JSON_DIR, STAGING_PARQUET_DIR, fs


@task(log_prints=True)
@coiled.function(
    name="data-etl",
    local=LOCAL,
    region=REGION,
    keepalive="5 minutes",
    tags={"workflow": "etl-tpch"},
)
def generate(scale: float, path: os.PathLike) -> None:
    static_tables = ["customer", "nation", "part", "partsupp", "region", "supplier"]
    with duckdb.connect() as con:
        con.install_extension("tpch")
        con.load_extension("tpch")

        if str(path).startswith("s3://"):
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
            if table in static_tables and (
                list((STAGING_JSON_DIR / table).rglob("*.json"))
                or list((STAGING_PARQUET_DIR / table).rglob("*.parquet"))
            ):
                print(f"Static table {table} already exists")
                continue
            print(f"Exporting table: {table}")
            stmt = f"""select * from {table}"""
            df = con.sql(stmt).arrow()

            outfile = (
                path
                / table
                / f"{table}_{datetime.datetime.now().isoformat().split('.')[0]}.json"
            )
            fs.makedirs(outfile.parent, exist_ok=True)
            df.to_pandas().to_json(
                outfile,
                date_format="iso",
                orient="records",
                lines=True,
            )
            print(f"Exported table {table} to {outfile}")
        print("Finished exporting all data")


@flow
def generate_data():
    generate(
        scale=0.01,
        path=STAGING_JSON_DIR,
    )