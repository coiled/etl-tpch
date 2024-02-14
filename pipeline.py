from datetime import timedelta

from prefect import serve

from pipeline.data import generate_data
from pipeline.monitor import check_model_endpoint
from pipeline.preprocess import json_to_parquet
from pipeline.reduce import query_reduce
from pipeline.resize import resize_parquet
from pipeline.train import update_model

if __name__ == "__main__":
    data = generate_data.to_deployment(
        name="generate_data",
        interval=timedelta(seconds=20),
    )
    preprocess = json_to_parquet.to_deployment(
        name="preprocess",
        interval=timedelta(seconds=30),
    )
    resize = resize_parquet.to_deployment(
        name="resize",
        interval=timedelta(seconds=30),
    )
    reduce = query_reduce.to_deployment(
        name="reduce",
        interval=timedelta(minutes=1),
    )
    train = update_model.to_deployment(
        name="train",
        interval=timedelta(minutes=2),
    )
    monitor = check_model_endpoint.to_deployment(
        name="monitor",
        interval=timedelta(seconds=10),
    )

    serve(
        data,
        preprocess,
        resize,
        reduce,
        train,
        monitor,
    )
