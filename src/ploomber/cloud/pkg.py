import sys
from pathlib import Path
from urllib.request import urlretrieve
from urllib import parse
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
from glob import glob
import zipfile
from pathlib import Path
from functools import wraps
from datetime import datetime
import json

import click
import requests
import humanize

from ploomber.table import Table

HOST = "https://lawqhyo5gl.execute-api.us-east-1.amazonaws.com/api/"


def _download_file(url):
    # remove leading /
    path = Path(parse.urlparse(url).path[1:])
    path.parent.mkdir(exist_ok=True, parents=True)
    print(f'Downloading {path}')
    urlretrieve(url, path)


def download_from_presigned(presigned):
    with ThreadPoolExecutor(max_workers=64) as executor:
        future2url = {
            executor.submit(_download_file, url=url)
            for url in presigned
        }

        for future in as_completed(future2url):
            exception = future.exception()

            if exception:
                task = future2url[future]
                raise RuntimeError(
                    'An error occurred when downloading product from '
                    f'task: {task!r}') from exception


def auth_header(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        api_key = os.environ.get("PLOOMBER_CLOUD_KEY")

        if api_key:
            headers = {
                "Authorization": api_key,
                "Content-Type": "application/json"
            }

            return func(headers, *args, **kwargs)

    return wrapper


@auth_header
def runs_new(headers, graph):
    return requests.post(f"{HOST}/runs/new", headers=headers,
                         json=graph).json()


@auth_header
def runs(headers):
    res = requests.get(f"{HOST}/runs", headers=headers).json()

    for run in res:
        run['created_at'] = humanize.naturaltime(
            datetime.fromisoformat(run['created_at']),
            when=datetime.utcnow(),
        )

    print(Table.from_dicts(res))


# NOTE: this doesn't need authentication (add unit test)
def tasks_update(task_id, status):
    response = requests.get(f"{HOST}/tasks/{task_id}/{status}")
    json_ = response.json()

    if response.status_code >= 300:
        print(
            f'Failed to update task (status: {response.status_code}): {json_}')
    else:
        print(
            f'Successfully updated task (status: {response.status_code}): {json_}'
        )

    return json_


@auth_header
def run_detail(headers, run_id):
    res = requests.get(f"{HOST}/runs/{run_id}", headers=headers).json()
    print(Table.from_dicts(res['tasks']))


@auth_header
def products_list(headers):
    res = requests.get(f"{HOST}/products", headers=headers).json()

    if res:
        print(Table.from_dicts([{'path': r} for r in res]))
    else:
        print("No products found.")


@auth_header
def products_download(headers, pattern):
    res = requests.get(f"{HOST}/products/{pattern}", headers=headers).json()
    download_from_presigned(res)


def zip_project(force):
    if Path("project.zip").exists():
        click.secho("Deleting existing project.zip...", fg="yellow")
        Path("project.zip").unlink()

    files = glob("**/*", recursive=True)

    # TODO: ignore __pycache__, ignore .git directory
    with zipfile.ZipFile("project.zip", "w", zipfile.ZIP_DEFLATED) as zip:
        for path in files:
            zip.write(path, arcname=path)

        zip.writestr('.ploomber-cloud', json.dumps({'force': force}))


@auth_header
def get_presigned_link(headers):
    return requests.get(f"{HOST}/upload", headers=headers).json()


def upload_zipped_project(response):
    with open("project.zip", "rb") as f:
        files = {"file": f}
        http_response = requests.post(response["url"],
                                      data=response["fields"],
                                      files=files)

    if http_response.status_code != 204:
        raise ValueError(f"An error happened: {http_response}")

    click.secho("Uploaded project, starting execution...", fg="green")


def upload_project(force):
    if not Path("requirements.lock.txt").exists():
        raise ValueError("missing requirements.lock.txt")

    click.echo("Zipping project...")
    zip_project(force)
    click.echo("Uploading project...")
    response = get_presigned_link()
    upload_zipped_project(response)
