import sys
import os
import asyncio
import functools
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import re
from zipfile import ZipFile

import boto3

from config import config


def set_paths() -> Path:
    cwd = os.getcwd()
    path = Path(cwd) / "s3-folder"
    path.mkdir(exist_ok=True)
    return path


def create_file(key: str) -> str:
    path = Path(os.getcwd()).joinpath("s3-folder", key)
    if key.endswith("/"):
        directory_path = path
    else:
        directory_path = path.parent

    if directory_path.is_dir():
        path.touch(exist_ok=True)
    else:
        directory_path.mkdir(parents=True, exist_ok=True)
        path.touch(exist_ok=True)

    return str(path)


def check_response(response):
    if response["KeyCount"] == 0:
        sys.exit("Folder  NOT Found With this Prefix : {}".format(
            response["Prefix"]))


def check_key(key: str, config):
    if re.match(config["pattern"], key):
        return True
    else:
        return False


def zip_file(path: Path):
    with ZipFile("s3-folder.zip", "a") as zip:
        zip.write(path)


async def main():
    start_time = time.time()
    s3 = boto3.client("s3")
    print(config)
    bucket_name = config["bucket"]
    executor = ThreadPoolExecutor(max_workers=5)
    process_executor = ProcessPoolExecutor(max_workers=4)
    set_paths()
    loop = asyncio.get_event_loop()

    def aio(f):
        async def io_wrapper(*args, **kwargs):
            bounded_f = functools.partial(f, *args, **kwargs)
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(executor, bounded_f)

        return io_wrapper

    download_file = aio(s3.download_file)
    s3_paginator = s3.get_paginator("list_objects_v2")
    s3_iterator = s3_paginator.paginate(
        Bucket=bucket_name, Prefix=config["prefix"], PaginationConfig={"PageSize": 100})

    tasks = []

    # check_response(response)
    for page in s3_iterator:
        for obj in page["Contents"]:
            if check_key(obj["Key"], config):
                path = create_file(obj["Key"])
                tasks.append(
                    (loop.create_task(download_file(bucket_name, obj["Key"], path)), obj["Key"]))
    zip_processes = []
    for task, key in tasks:
        await task
        print(f"{key} gonna zip")
        zip_processes.append(process_executor.submit(zip_file,Path("s3-folder").joinpath(key)))
        print(f"{key}  zipped.")

    print("--- %s seconds ---" % (time.time() - start_time))


if __name__ == '__main__':
    asyncio.run(main())
