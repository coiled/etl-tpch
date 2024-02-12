from pydoc import locate

import numpy as np
import uvicorn
import xgboost as xgb
from fastapi import FastAPI
from pydantic import BaseModel

from pipeline.files import MODEL_FILE, fs

model = xgb.XGBClassifier()
with fs.open(MODEL_FILE, mode="rb") as f:
    model.load_model(bytearray(f.read()))


def create_type_instance(type_name: str):
    return locate(type_name).__call__()


def get_features_dict(model):
    feature_names = model.get_booster().feature_names
    feature_types = list(map(create_type_instance, model.get_booster().feature_types))
    return dict(zip(feature_names, feature_types))


def create_input_features_class(model):
    return type("InputFeatures", (BaseModel,), get_features_dict(model))


InputFeatures = create_input_features_class(model)
app = FastAPI()


@app.get("/predict", response_model=list)
async def predict_post(datas: list[InputFeatures]):
    return model.predict(
        np.asarray([list(data.__dict__.values()) for data in datas])
    ).tolist()


@app.get("/health")
async def service_health():
    """Return service health"""
    return {"ok"}


if __name__ == "__main__":
    print(get_features_dict(model))
    uvicorn.run(app, host="0.0.0.0", port=8080)
