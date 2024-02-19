Example Production ETL System - TPC-H
=====================================

This is an example system that runs regular jobs on a schedule, at scale, on
the cloud using a variety of technologies:

-  **Prefect:** for job scheduling and workflow management
-  **Coiled::** for cloud hardware provisioning
-  **Dask:** for parallel processing
-  **Parquet** and **Deltalake:** for data storage
-  **XGBoost:** for model training
-  **Streamlit** and **Fast API:** for dashboarding and model hosting

Raw data flows in every minute and is transformed through several stages at
different scales and cadences.

-  **Data Generation:** (*every 30 seconds*) new JSON files appear
-  **Data Preprocessing:** (every minute) JSON gets transformed into Parquet / Delta
-  **Data Compaction:** (every 30 minutes) small Parquet files get repartitioned into larger ones
-  **Business Queries:** (every hour) large scale multi-table analysisqueries run that feed dashboards
-  **Model Training:** (every six hours) with XGBoost

Additionally we keep Streamlit and FastAPI servers up and running.

It looks kinda like this:

![ETL Pipeline](images/excalidraw.png)

How this works
--------------

### Concurrent Flows

The file [workflow.py](workflow.py) defines several Prefect flows operating at
different cadences:

```python
# workflow.py
...

generate = generate_json.to_deployment(
    name="generate_data",
    interval=timedelta(seconds=30),
)
preprocess = json_to_parquet.to_deployment(
    name="preprocess",
    interval=timedelta(minutes=1),
)
train = update_model.to_deployment(
    name="train",
    interval=timedelta(hours=6),
)

prefect.serve(
    generate,
    preprocess,
    train,
    ...
)
```

### Coiled Functions + Prefect Tasks

These flows are defined in files like [pipeline/preprocess.py](pipeline/preprocess.py), which combine prefect tasks either with Coiled functions for remote execution like the following:

```python
# pipeline/preprocess.py

import coiled
from prefect import task, flow
import pandas as pd

@task
@coiled.function(region="us-east-2", memory="64 GiB")
def json_to_parquet(filename):
    df = pd.read_json(filename)
    df.to_parquet(OUTFILE / filename.split(".")[-2] + ".parquet")

@flow
def convert_all_files():
    files = list_files()
    json_to_parquet.map(files)
```

### Dask Clusters for larger jobs

Or in files like [pipeline/reduce.py](pipeline/reduce.py) which create larger
clusters on-demand.

```python
# pipeline/reduce.py

import coiled
from prefect import task, flow
import dask.dataframe as dd

@task
def query_for_bad_accounts(bucket):
    with coiled.Cluster(n_workers=20, region="us-east-2") as cluster:
        with cluster.get_client() as client:
            df = dd.read_parquet(bucket)

            ... # complex query

            result.to_parquet(...)
```

Data Generation
---------------

In the background we're generating the data ourselves.  This data is taken from
the TPC-H benchmark suite.  It's a bunch of tables that simulate orders /
customers / suppliers, and has a schema that looks like this:

![TPC-H Schema](https://docs.snowflake.com/en/_images/sample-data-tpch-schema.png)

This gives the system a sense of realism, while still being able to tune up or
down in scale and run easily as an example.

How to Run Locally
------------------

Make sure you have a [Prefect cloud](https://www.prefect.io/cloud) account and have authenticated your local machine.

Create a software environment:

```bash
mamba env create -f environment.yml
```

Then run each command below in separate terminal windows:

```bash
python serve_model.py       # Serve ML model
```
```bash
python workflow.py          # Run ETL pipeline
```
```bash
streamlit run dashboard.py  # Serve dashboard
```

Watch things run, both in your terminals and on Prefect cloud, and then ctrl-C to shut everything down.

How to Run in the Cloud
-----------------------

This all works, we just haven't documented it well yet.
