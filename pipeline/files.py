import fsspec
from upath import UPath as Path

from .settings import LOCAL

if LOCAL:
    ROOT = Path(__file__).parent.parent.resolve() / "data"
    fs = fsspec.filesystem("local")
else:
    ROOT = Path("s3://oss-scratch-space/jrbourbeau/etl-tpch/data")
    fs = fsspec.filesystem("s3")

STAGING_JSON_DIR = ROOT / "staged" / "json"
STAGING_PARQUET_DIR = ROOT / "staged" / "parquet"
RAW_JSON_DIR = ROOT / "raw" / "json"
RAW_PARQUET_DIR = ROOT / "raw" / "parquet"
PROCESSED_DATA_DIR = ROOT / "processed"
REDUCED_DATA_DIR = ROOT / "reduced"
MODEL_FILE = ROOT.parent / "model.json"
