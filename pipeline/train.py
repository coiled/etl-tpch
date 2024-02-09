import os

import pandas as pd
import xgboost as xgb
from prefect import flow, task

from .files import MODEL_FILE, REDUCED_DATA_DIR


@task
def train(model):
    df = pd.read_parquet(os.path.join(REDUCED_DATA_DIR, "europe", "brass"))
    X = df[["p_partkey", "s_acctbal"]]
    y = df["n_name"].map(
        {"FRANCE": 0, "UNITED KINGDOM": 1, "RUSSIA": 2, "GERMANY": 3, "ROMANIA": 4}
    )
    model.fit(X, y)
    return model


@flow(log_prints=True)
def update_model():
    model = xgb.XGBClassifier()
    if os.path.exists(MODEL_FILE):
        model.load_model(MODEL_FILE)
    model = train(model)
    model.save_model(MODEL_FILE)
    print(f"Updated model at {MODEL_FILE}")
