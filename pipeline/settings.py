import boto3
import fsspec
from filelock import FileLock
from upath import UPath as Path

LOCAL = True
# LOCAL = False

if LOCAL:
    ROOT = Path(__file__).parent.parent.resolve() / "data"
    fs = fsspec.filesystem("local")
    REGION = None
    storage_options = {}
else:
    # TODO: Make the cloud path nicer (e.g. s3://coiled-datasets-rp)
    ROOT = Path("s3://openscapes-scratch/jrbourbeau/etl-tpch/data")
    fs = fsspec.filesystem("s3", use_listings_cache=False)
    # Find cloud region being used
    bucket = str(ROOT).replace("s3://", "").split("/")[0]
    resp = boto3.client("s3").get_bucket_location(Bucket=bucket)
    REGION = resp["LocationConstraint"] or "us-east-1"
    storage_options = {"AWS_REGION": REGION, "AWS_S3_ALLOW_UNSAFE_RENAME": "true"}

STAGING_JSON_DIR = ROOT / "staged" / "json"
STAGING_PARQUET_DIR = ROOT / "staged" / "parquet"
RAW_JSON_DIR = ROOT / "raw" / "json"
RAW_PARQUET_DIR = ROOT / "raw" / "parquet"
PROCESSED_DATA_DIR = ROOT / "processed"
REDUCED_DATA_DIR = ROOT / "reduced"
MODEL_FILE = ROOT.parent / "model.json"
MODEL_SERVER_FILE = ROOT.parent / "serve_model.py"

lock_generate = FileLock("generate.lock")
lock_json_to_parquet = FileLock("json_to_parquet.lock")
lock_compact = FileLock("compact.lock")
