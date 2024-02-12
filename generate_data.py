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
from prefect import flow

from pipeline.files import STAGING_JSON_DIR, fs
from pipeline.settings import LOCAL

REGION = None


class CompressionCodec(enum.Enum):
    SNAPPY = "snappy"
    LZ4 = "lz4"
    ZSTD = "zstd"
    GZIP = "gzip"
    BROTLI = "brotli"
    NONE = None


@coiled.function(local=LOCAL, region="us-east-1")
def generate(
    scale: float = 0.1,
    path: str = "./tpch-data",
    relaxed_schema: bool = False,
    compression: CompressionCodec = CompressionCodec.NONE,
) -> str:
    if str(path).startswith("s3"):
        path += "/" if not path.endswith("/") else ""
        global REGION
        REGION = get_bucket_region(path)
    else:
        path = pathlib.Path(path)
        # path = pathlib.Path(path)
        path.mkdir(parents=True, exist_ok=True)

    print(f"Scale: {scale}, Path: {path}")

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

        if relaxed_schema:
            print("Converting types date -> timestamp_s and decimal -> double")
            _alter_tables(con)
            print("Done altering tables")

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

            file = f"{table}_{datetime.datetime.now().isoformat()}.json"
            if isinstance(out, str) and out.startswith("s3"):
                out_ = f"{out}/{file}"
            else:
                out_ = pathlib.Path(out)
                out_.mkdir(exist_ok=True, parents=True)
                out_ = str(out_ / file)

            df.to_pandas().to_json(
                out_,
                compression=compression.value,
                date_format="iso",
                orient="records",
                lines=True,
            )
            print(f"Finished exporting table {table}! to {out_}")
        print("Finished exporting all data!")


def _alter_tables(con):
    """
    Temporary, used for debugging performance in data types.

    ref discussion here: https://github.com/coiled/benchmarks/pull/1131
    """
    tables = [
        "nation",
        "region",
        "customer",
        "supplier",
        "lineitem",
        "orders",
        "partsupp",
        "part",
    ]
    for table in tables:
        schema = con.sql(f"describe {table}").arrow()

        # alter decimals to floats
        for column in schema.filter(
            pc.match_like(pc.field("column_type"), "DECIMAL%")
        ).column("column_name"):
            con.sql(f"alter table {table} alter {column} type double")

        # alter date to timestamp_s
        for column in schema.filter(pc.field("column_type") == "DATE").column(
            "column_name"
        ):
            con.sql(f"alter table {table} alter {column} type timestamp_s")


def get_bucket_region(path: str):
    if not path.startswith("s3://"):
        raise ValueError(f"'{path}' is not an S3 path")
    bucket = path.replace("s3://", "").split("/")[0]
    resp = boto3.client("s3").get_bucket_location(Bucket=bucket)
    # Buckets in region 'us-east-1' results in None, b/c why not.
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/get_bucket_location.html#S3.Client.get_bucket_location
    return resp["LocationConstraint"] or "us-east-1"


@flow(log_prints=True)
def generate_data(data_dir):
    fs.makedirs(data_dir, exist_ok=True)
    generate(
        scale=0.01,
        path=data_dir,
        relaxed_schema=True,
    )


if __name__ == "__main__":
    generate_data.serve(
        name="generate_data",
        parameters={"data_dir": STAGING_JSON_DIR},
        interval=timedelta(seconds=20),
    )
