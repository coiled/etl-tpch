# Easy Scalable Production ETL

**This repository is a lightweight scalable example pipeline that runs large Python jobs on a schedule in the cloud. We hope this example is easy to copy and modify for your own needs.**

Learn more in our [blog post](https://docs.coiled.io/blog/easy-scalable-production-etl.html?utm_source=github&utm_medium=etl).

## Background

It’s common to run regular large-scale Python jobs on the cloud as part of production data pipelines. Modern workflow orchestration systems like Prefect, Dagster, Airflow, Argo, etc. all work well for running jobs on a regular cadence, but we often see groups struggle with complexity around cloud infrastructure and lack of scalability. 

This repository contains a scalable data pipeline that runs regular jobs on the cloud with [Coiled](https://docs.coiled.io/user_guide/index.html) and Prefect. This approach is:

- **Easy to deploy** on the cloud
- **Scalable** across many cloud machines
- **Cheap to run** on ephemeral VMs and a small always-on VM

![ETL Pipeline](images/excalidraw.png)

## How to run

_You can run this pipeline yourself, either locally or on the cloud._

Make sure you have a [Prefect cloud](https://www.prefect.io/cloud) account and have authenticated your local machine.

Clone this repository and install dependencies:

```bash
git clone https://github.com/coiled/etl-tpch
cd etl-tpch
mamba env create -f environment.yml
mamba activate etl-tpch
```

### Local

In your terminal run:

```bash
python workflow.py   # Run data pipeline locally
```

### Cloud

If you haven't already, create a Coiled account and follow the setup
guide at [coiled.io/start](https://coiled.io/start).

Next, adjust the [`pipeline/config.yml`](pipeline/config.yml)
configuration file by setting `local: false` and `data-dir` to an
S3 bucket where you would like data assets to be stored.

Set `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` and `AWS_REGION`
environment variables that enable access to your S3 bucket and
specify the region the bucket is in, respectively.

```bash
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_REGION=...
```

Finally, in your terminal run:

```bash
coiled prefect serve \ 
    --vm-type t3.medium \                             # Small, always-on VM
    --region $AWS_REGION \                            # Same region as data
    -f dashboard.py -f pipeline \                     # Include pipeline files
    -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \         # S3 bucket access
    -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
    workflow.py
```
