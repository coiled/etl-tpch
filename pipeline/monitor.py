import requests
from prefect import flow


@flow
def check_model_endpoint():
    r = requests.get("http://0.0.0.0:8080/health")
    if not r.json() == ["ok"]:
        raise ValueError("Model endpoint isn't healthy")
