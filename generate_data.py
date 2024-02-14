import datetime
import os
import pathlib
from datetime import timedelta

import botocore.session
import coiled
import duckdb
import psutil
from prefect import flow, task

from pipeline.settings import LOCAL, REGION, STAGING_JSON_DIR, fs


@task(log_prints=True)
@coiled.function(
    local=LOCAL,
    region=REGION,
    tags={"workflow": "etl-tpch"},
)
def generate(scale: float, path: os.PathLike) -> None:
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
        else:
            path = pathlib.Path(path)
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


if __name__ == "__main__":
    generate_data.serve(
        name="generate_data",
        interval=timedelta(seconds=20),
    )
