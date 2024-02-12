import tempfile

import coiled
import pandas as pd
import xgboost as xgb
from prefect import flow, task

from .settings import MODEL_FILE, REDUCED_DATA_DIR, Path, coiled_options, fs


@task
@coiled.function(**coiled_options)
def train():
    df = pd.read_parquet(REDUCED_DATA_DIR / "europe" / "brass")
    X = df[["p_partkey", "s_acctbal"]]
    y = df["n_name"].map(
        {"FRANCE": 0, "UNITED KINGDOM": 1, "RUSSIA": 2, "GERMANY": 3, "ROMANIA": 4}
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


@flow(log_prints=True)
def update_model():
    train()
    print(f"Updated model at {MODEL_FILE}")
