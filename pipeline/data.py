import datetime
import os

import coiled
import duckdb
import psutil
from dask.distributed import print
from prefect import flow, task

from .settings import LOCAL, PROCESSED_DIR, REGION, STAGING_DIR, fs, lock_generate


@task(log_prints=True)
@coiled.function(
    name="data-generation",
    local=LOCAL,
    region=REGION,
    vm_type="m6i.2xlarge",
    tags={"workflow": "etl-tpch"},
)
def generate(scale: float, path: os.PathLike) -> None:
    static_tables = ["customer", "nation", "part", "partsupp", "region", "supplier"]
    with duckdb.connect() as con:
        con.install_extension("tpch")
        con.load_extension("tpch")
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
                list((STAGING_DIR / table).rglob("*.json"))
                or list((PROCESSED_DIR / table).rglob("*.parquet"))
            ):
                print(f"Static table {table} already exists")
                continue
            print(f"Exporting table: {table}")
            stmt = f"""select * from {table}"""
            df = con.sql(stmt).df()
            # TODO: Increment the order key in the `lineitem` and `orders`
            # tables each time the flow is run to produce unique transactions
            # xref https://discourse.prefect.io/t/how-to-get-flow-count/3996
            # if table in ["lineitem", "orders"]:
            #     df[f"{table[0]}_orderkey"] += counter

            outfile = (
                path
                / table
                / f"{table}_{datetime.datetime.now().isoformat().split('.')[0]}.json"
            )
            fs.makedirs(outfile.parent, exist_ok=True)
            df.to_json(
                outfile,
                date_format="iso",
                orient="records",
                lines=True,
            )
            print(f"Exported table {table} to {outfile}")
        print("Finished exporting all data")


@flow
def generate_data():
    with lock_generate:
        generate(
            scale=1,
            path=STAGING_DIR,
        )
