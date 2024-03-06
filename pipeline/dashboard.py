import contextlib
import os
import shlex
import subprocess

import coiled
from rich import print

from .settings import DASHBOARD_FILE, LOCAL


@contextlib.contextmanager
def serve_dashboard():
    try:
        print("[green]Deploying dashboard...[/green]")
        port = 8080
        cmd = f"streamlit run {DASHBOARD_FILE} --server.port {port} --server.headless true"
        if LOCAL:
            p = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE)
            address = f"http://0.0.0.0:{port}"
        else:
            name = "etl-tpch-dashboard"
            subdomain = "etl-tpch"
            cmd = f"""
            coiled run \
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
            with coiled.Cloud() as cloud:
                account = cloud.default_account
            address = f"http://{subdomain}.{account}.dask.host:{port}"
        print(f"Dashboard is available at [blue]{address}[/blue] :rocket:")
        yield
    finally:
        if LOCAL:
            p.kill()
        else:
            coiled.delete_cluster(name=name)
