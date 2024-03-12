# Easy Scalable Production ETL - TPC-H

**This repository is a lightweight scalable example pipeline that runs large Python jobs on a schedule in the cloud.**

We hope this example is easy to copy and modify for your needs.

## Data Pipelines

*How do I process large scale data continuously in production?*

While specific details vary across use cases, we often see these four steps:

- Step 1: **Acquire raw data** from the real world
    - _Example:_ A customer buys a load of bread at your grocery store
    - _Example:_ A satellite measurements sea temperature in Antarctica
- Step 2: **Regularly process** that raw data
    - _Example:_ Apply data quality cuts followed by a feature engineering step
    - _Example:_ Convert to a different storage format
    - _Extra requirement_: Retry on failures, run close to data in the cloud, alert me if processing fails
- Step 3: **Scalably process** that data periodically
    - _Example:_ Large scale query of your data -- "How many purchases were returned at our store in NYC?"
    - _Example:_ Train an ML model for weather forecasting
    - _Extra requirement_: Data is too large to fit on a single machine
- Step 4: **Publish results**
    - _Example:_ Serve an interactive dashboard with a real-time view of customer orders
    - _Example:_ Host an ML model

Running these steps on a regular cadence can be handled by most modern workflow management systems (e.g. Prefect, Dagster, Airflow, Argo). Managing cloud infrastructure and scalability is where most people tend to struggle.


## Pain points of common data pipelines

_Pipelines usually either don't scale well or are overly complex._

Most modern workflow management systems (e.g. Prefect, Dagster, Airflow, Argo) are able to address workflow orchestration needs well. Instead, where we tend to see groups struggle is with:

- **Complexity** around managing cloud infrastructure:
    - Provisioning / deprovisioning cloud machines
    - Software environment management
    - Handling cloud data access
    - Easy access to logs
    - Cost monitoring and spending limits
- **Scale** limitations:
    - Scaling existing Python code across many cloud machines in parallel
    - Computing on larger-than-memory datasets (e.g. 100 TB Parquet dataset in S3)

Because of these issues, it's common for systems to be overly complex or very expensive.

Below we show an example that connects Prefect and Coiled to run a lightweight, scalable data pipeline on the cloud.


## Solution in this repository

_Coiled and Prefect together make it easy to run regularly scheduled jobs on the cloud at scale._

Our example data pipeline looks like the following:

- Step 1: **Data generation** &mdash; New [TPC-H dataset](https://www.tpc.org/tpch/) JSON files with customer orders and supplier information appear in an S3 bucket (every 15 minutes)
- Step 2: **Regular processing**
    - JSON gets transformed into Parquet / Delta (every 15 minutes)
    - Data compaction of small Parquet files into larger ones for efficient downstream usage (every 6 hours)
- Step 3: **Scalable processing** &mdash; Run large scale multi-table analysis query to monitor unshipped, high-revenue orders (every 24 hours)
- Step 4: **Serve dashboard** &mdash; Results from latest business query are served on a dashboard (always on)

![ETL Pipeline](images/excalidraw.png)

## How this works

_We combine Prefect's workflow orchestration with Coiled's easy cloud infrastructure management._

We think this is a lightweight approach that addresses common pain points and works well for most people.

### Define tasks and hardware in Python

Data-intensive tasks, like those in [pipeline/preprocess.py](pipeline/preprocess.py), are combined with Coiled Functions for remote execution on cloud VMs.

Coiled makes it easy to deploy Prefect tasks on cloud hardware of our choosing.

```python
# pipeline/preprocess.py

import coiled
from prefect import task, flow
import pandas as pd

@task
@coiled.function(region="us-east-2", memory="64 GiB")  # <--- Define hardware in script
def json_to_parquet(filename):
    df = pd.read_json(filename)                        # <--- Use simple pandas functions
    df.to_parquet(OUTDIR / filename.split(".")[-2] + ".parquet")

@flow
def convert_all_files():
    files = list_files()
    json_to_parquet.map(files)                         # <--- Map across many files at once
```


### Run jobs regularly

The file [workflow.py](workflow.py) defines a Prefect flow for each step in our pipeline and runs them regularly at different cadences.

Prefect makes it easy to schedule regular jobs and manage workflow orchestration.

```python
# workflow.py

from datetime import timedelta
from prefect import serve

preprocess = json_to_parquet.to_deployment(
    name="preprocess",
    interval=timedelta(minutes=15),             # <--- Run job on a regular schedule
)
reduce = query_reduce.to_deployment(
    name="reduce",
    interval=timedelta(hours=24),               # <--- Different jobs at different schedules
)

...

serve(
    data,
    preprocess,
    reduce,
    ...
)
```


### Scale with Dask Dask Clusters

The large-scale, multi-table analysis query in [pipeline/reduce.py](pipeline/reduce.py) uses Coiled to create an on-demand Dask cluster to handle this large-scale processing.

Dask handles larger-than-memory computations. Coiled deploys Dask for us.

```python
# pipeline/reduce.py

import coiled
from prefect import task, flow
import dask.dataframe as dd

@task
def unshipped_orders_by_revenue(bucket):
    with coiled.Cluster(
        n_workers=200,                  # <--- Ask for hundreds of workers
        region="us-west-2",             # <--- Run anywhere on any hardware
    ) as cluster:
        with cluster.get_client() as client:
            df = dd.read_parquet(bucket)
            result = ...   # Complex query
            result.to_parquet(...)
```

## How to run

_You can run this pipeline yourself, either locally or on the cloud._

Make sure you have a [Prefect cloud](https://www.prefect.io/cloud) account and have authenticated your local machine.

Create a software environment:

```bash
mamba env create -f environment.yml
mamba activate etl-tpch
```

### Local

In your terminal run:

```bash
python workflow.py   # Run data pipeline locally
```

### Cloud

In your terminal run:

```bash
coiled prefect serve \
    --region us-west-2 \                              # Same region as data
    --vm-type t3.medium \                             # Small, always-on VM
    -f dashboard.py -f pipeline \                     # Include files
    -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \         # AWS credentials
    -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
    workflow.py
```
