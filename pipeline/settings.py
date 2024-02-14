import boto3
import fsspec
from upath import UPath as Path

LOCAL = True
# LOCAL = False

if LOCAL:
    ROOT = Path(__file__).parent.parent.resolve() / "data"
    fs = fsspec.filesystem("local")
    REGION = None
else:
    # TODO: Make the cloud path nicer (e.g. s3://coiled-datasets-rp)
    ROOT = Path("s3://oss-scratch-space/jrbourbeau/etl-tpch/data")
    fs = fsspec.filesystem("s3")
    # Find cloud region being used
    bucket = str(ROOT).replace("s3://", "").split("/")[0]
    resp = boto3.client("s3").get_bucket_location(Bucket=bucket)
    REGION = resp["LocationConstraint"] or "us-east-1"

STAGING_JSON_DIR = ROOT / "staged" / "json"
STAGING_PARQUET_DIR = ROOT / "staged" / "parquet"
RAW_JSON_DIR = ROOT / "raw" / "json"
RAW_PARQUET_DIR = ROOT / "raw" / "parquet"
PROCESSED_DATA_DIR = ROOT / "processed"
REDUCED_DATA_DIR = ROOT / "reduced"
MODEL_FILE = ROOT.parent / "model.json"
MODEL_SERVER_FILE = ROOT.parent / "serve_model.py"
