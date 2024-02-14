from datetime import timedelta

from prefect import serve

from pipeline.monitor import check_model_endpoint
from pipeline.preprocess import compact_tables, json_to_parquet
from pipeline.reduce import query_reduce
from pipeline.train import update_model

if __name__ == "__main__":
    preprocess = json_to_parquet.to_deployment(
        name="preprocess",
        interval=timedelta(seconds=60),
    )
    compact = compact_tables.to_deployment(
        name="compact",
        interval=timedelta(minutes=5),
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
        preprocess,
        compact,
        reduce,
        train,
        monitor,
    )
