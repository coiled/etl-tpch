import boto3
import fsspec
import yaml
from filelock import FileLock
from upath import UPath as Path

with open(Path(__file__).parent / "config.yml", "rb") as f:
    data = yaml.safe_load(f)

LOCAL = data["local"]
ROOT = Path(data["data-dir"]).resolve()
fs = fsspec.filesystem(ROOT.protocol, use_listings_cache=False)

if LOCAL:
    REGION = None
    storage_options = {}
else:
    bucket = str(ROOT).replace("s3://", "").split("/")[0]
    resp = boto3.client("s3").get_bucket_location(Bucket=bucket)
    REGION = resp["LocationConstraint"] or "us-east-1"
    storage_options = {"AWS_REGION": REGION, "AWS_S3_ALLOW_UNSAFE_RENAME": "true"}

STAGING_DIR = ROOT / "staging"  # Input JSON files
PROCESSED_DIR = ROOT / "processed"  # Processed Parquet files
RESULTS_DIR = ROOT / "results"  # Reduced/aggrgated results
ARCHIVE_DIR = ROOT / "archive"  # Archived JSON files
MODEL_FILE = ROOT.parent / "model.json"
MODEL_SERVER_FILE = ROOT.parent / "serve_model.py"

lock_dir = Path(__file__).parent.parent / ".locks"
lock_generate = FileLock(lock_dir / "generate.lock")
lock_json_to_parquet = FileLock(lock_dir / "json.lock")
lock_compact = FileLock(lock_dir / "compact.lock")
