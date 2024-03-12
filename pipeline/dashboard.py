import os
import shlex
import subprocess

import coiled
import requests
from prefect import flow
from rich import print

from .settings import DASHBOARD_FILE, LOCAL, REGION

port = 8080
name = "etl-tpch-dashboard"
subdomain = "etl-tpch"


def deploy():
    print("[green]Deploying dashboard...[/green]")
    cmd = f"streamlit run {DASHBOARD_FILE} --server.port {port} --server.headless true"
    if LOCAL:
        subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE)
    else:
        cmd = f"""
        coiled run \
            --region {REGION} \
            --vm-type t3.medium \
            -f dashboard.py \
            -f pipeline \
            --subdomain {subdomain} \
            --port {port} \
            -e AWS_ACCESS_KEY_ID={os.environ['AWS_ACCESS_KEY_ID']} \
            -e AWS_SECRET_ACCESS_KEY={os.environ['AWS_SECRET_ACCESS_KEY']} \
            --detach \
            --keepalive '520 weeks' \
            --name {name} \
            -- \
            {cmd}
        """
        subprocess.run(shlex.split(cmd))
    print(f"Dashboard is available at [blue]{get_address()}[/blue] :rocket:")


def get_address():
    if LOCAL:
        return f"http://0.0.0.0:{port}"
    else:
        with coiled.Cloud() as cloud:
            account = cloud.default_account
        return f"http://{subdomain}.{account}.dask.host:{port}"


@flow(log_prints=True)
def deploy_dashboard():
    address = get_address()
    try:
        r = requests.get(address)
        r.raise_for_status()
    except Exception:
        deploy()
    else:
        print("Dashboard is healthy")
