import sys
import os
import asyncio
import functools
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import threading
from configparser import ConfigParser
import re

import boto3

from config import config
#  Parse Config.ini file
#  Add prefix
# TODO Add regex
# TODO add sql query
# TODO add paginator
# TODO zip dowloanded files
# TODO multiprocess for zip files


def set_paths() -> None:
    cwd = os.getcwd()
    path = Path(cwd) / "s3-folder"
    path.mkdir(exist_ok=True)
    return path


def create_file(key: str) -> None:
    path = Path(os.getcwd()).joinpath("s3-folder", key)
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


async def main():
    s3 = boto3.client("s3")
    print(config)
    bucket_name = config["bucket"]
    executor = ThreadPoolExecutor(max_workers=5)
    set_paths()
    loop = asyncio.get_event_loop()

    def aio(f):
        async def io_wrapper(*args, **kwargs):
            bounded_f = functools.partial(f, *args, **kwargs)
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(executor, bounded_f)
        return io_wrapper

    list_objects_v2 = aio(s3.list_objects_v2)
    download_file = aio(s3.download_file)

    response = await list_objects_v2(Bucket=bucket_name, Prefix=config["prefix"])
    tasks = []

    check_response(response)

    for obj in response["Contents"]:
        path = create_file(obj["Key"])
        if check_key(obj["Key"], config):
            tasks.append(
                (loop.create_task(download_file(bucket_name, obj["Key"], path)), obj["Key"]))

    for task, key in tasks:
        await task
        print("Downloaded : {}".format(key))

if __name__ == '__main__':
    asyncio.run(main())
