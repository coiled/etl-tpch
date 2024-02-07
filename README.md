# ETL-TPCH

Make sure you have a [Prefect cloud](https://www.prefect.io/cloud) account and have authenticated your local machine.

Create a software environment:

```bash
mamba env create -f environment.yml
```

Then start regularly generating TPC-H data locally:

```bash
python generate_data.py
```

Then, in a separate terminal window, serve our ML model:

```bash
python serve_model.py
```

Then, in yet another terminal window, run the ETL pipeline:

```bash
python pipeline.py
```

Watch things run, both in your terminals and on Prefect cloud, and then ctrl-C to shut everything down.