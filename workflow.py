from datetime import timedelta

from prefect import serve

from pipeline.dashboard import deploy_dashboard
from pipeline.data import generate_data
from pipeline.preprocess import compact_tables, json_to_parquet
from pipeline.reduce import query_reduce

if __name__ == "__main__":

    data = generate_data.to_deployment(
        name="generate_data",
        interval=timedelta(minutes=15),
    )
    preprocess = json_to_parquet.to_deployment(
        name="preprocess",
        interval=timedelta(minutes=15),
    )
    compact = compact_tables.to_deployment(
        name="compact",
        interval=timedelta(hours=6),
    )
    reduce = query_reduce.to_deployment(
        name="reduce",
        interval=timedelta(days=1),
    )
    dashboard = deploy_dashboard.to_deployment(
        name="dashboard",
        interval=timedelta(minutes=5),
    )
    deploy_dashboard()
    serve(
        data,
        preprocess,
        compact,
        reduce,
        dashboard,
    )
