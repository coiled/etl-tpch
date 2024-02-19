from datetime import timedelta

from prefect import serve

from pipeline.data import generate_data
from pipeline.monitor import check_model_endpoint
from pipeline.preprocess import compact_tables, json_to_parquet
from pipeline.reduce import query_reduce
from pipeline.train import update_model

if __name__ == "__main__":
    data = generate_data.to_deployment(
        name="generate_data",
        interval=timedelta(seconds=30),
    )
    preprocess = json_to_parquet.to_deployment(
        name="preprocess",
        interval=timedelta(minutes=1),
    )
    compact = compact_tables.to_deployment(
        name="compact",
        interval=timedelta(minutes=30),
    )
    reduce = query_reduce.to_deployment(
        name="reduce",
        interval=timedelta(hours=1),
    )
    train = update_model.to_deployment(
        name="train",
        interval=timedelta(hours=6),
    )
    monitor = check_model_endpoint.to_deployment(
        name="monitor",
        interval=timedelta(minutes=1),
    )

    serve(
        data,
        preprocess,
        compact,
        reduce,
        train,
        # monitor,
    )
