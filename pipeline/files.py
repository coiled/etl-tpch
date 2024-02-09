import fsspec
from upath import UPath as Path

from .settings import LOCAL

if LOCAL:
    ROOT = Path(__file__).parent.parent.resolve() / "data"
else:
    ROOT = Path("s3://oss-scratch-space/jrbourbeau/etl-tpch/data")

STAGING_JSON_DIR = ROOT / "staged" / "json"
STAGING_PARQUET_DIR = ROOT / "staged" / "parquet"
RAW_JSON_DIR = ROOT / "raw" / "json"
RAW_PARQUET_DIR = ROOT / "raw" / "parquet"
PROCESSED_DATA_DIR = ROOT / "processed"
REDUCED_DATA_DIR = ROOT / "reduced"
MODEL_FILE = (ROOT / "../" / "model.json").resolve()


def get_filesystem(path):
    protocol = fsspec.utils.get_protocol(path)
    return fsspec.filesystem(protocol)
