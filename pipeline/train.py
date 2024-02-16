import tempfile

import coiled
import pandas as pd
import xgboost as xgb
from prefect import flow, task

from .settings import LOCAL, MODEL_FILE, REDUCED_DATA_DIR, REGION, Path, fs


@task
@coiled.function(
    name="train",
    local=LOCAL,
    region=REGION,
    tags={"workflow": "etl-tpch"},
)
def train(file):
    df = pd.read_parquet(file)
    X = df[["p_partkey", "s_acctbal"]]
    y = df["n_name"].map(
        {"GERMANY": 0, "ROMANIA": 1, "RUSSIA": 2, "FRANCE": 3, "UNITED KINGDOM": 4}
    )
    model = xgb.XGBClassifier()
    if MODEL_FILE.exists():
        with fs.open(MODEL_FILE, mode="rb") as f:
            model.load_model(bytearray(f.read()))
    model.fit(X, y)

    # `save_model` only accepts local file paths
    with tempfile.TemporaryDirectory() as tmpdir:
        out = Path(tmpdir) / "model.json"
        model.save_model(out)
        fs.mv(str(out), str(MODEL_FILE.parent))
    return model


def list_training_data_files():
    data_dir = REDUCED_DATA_DIR / "europe" / "brass"
    return list(data_dir.rglob("*.parquet"))


@flow(log_prints=True)
def update_model():
    files = list_training_data_files()
    if not files:
        print("No training data available")
        return
    train(files[0])
    print(f"Updated model at {MODEL_FILE}")
