# ETL-TPCH

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
