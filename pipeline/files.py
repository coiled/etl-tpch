import os
import pathlib

ROOT = pathlib.Path(__file__).parent.parent.resolve() / "data"
STAGING_JSON_DIR = os.path.join(ROOT, "staged", "json")
STAGING_PARQUET_DIR = os.path.join(ROOT, "staged", "parquet")
RAW_JSON_DIR = os.path.join(ROOT, "raw", "json")
RAW_PARQUET_DIR = os.path.join(ROOT, "raw", "parquet")
PROCESSED_DATA_DIR = os.path.join(ROOT, "processed")
REDUCED_DATA_DIR = os.path.join(ROOT, "reduced")
MODEL_FILE = (ROOT / "../" / "model.json").resolve()
