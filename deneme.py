import sys
import os
import asyncio
import functools
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
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
    print(f"Zipped :: {str(path)}", flush=True)


async def download_and_zip(response: dict, session: dict):
    def aio(f):
        async def io_wrapper(*args, **kwargs):
            bounded_f = functools.partial(f, *args, **kwargs)
            loop = session["loop"]
            return await loop.run_in_executor(session["thread_executor"], bounded_f)

        return io_wrapper

    download_file = aio(session["s3"].download_file)

    tasks = []
    for obj in response["Contents"]:
        if check_key(obj["Key"], config):
            path = create_file(obj["Key"])
            tasks.append(
                (session["loop"].create_task(download_file(config["bucket"], obj["Key"], path)), obj["Key"]))
    zip_processes = []
    for task, key in tasks:
        await task
        print(f"{key} gonna zip")
        zip_processes.append(session["process_executor"].submit(zip_file, Path("s3-folder").joinpath(key)))

    for _ in as_completed(zip_processes):
        pass


def set_session() -> dict:
    return dict(
        s3=boto3.client("s3"),
        start_time=time.time(),
        thread_executor=ThreadPoolExecutor(max_workers=5),
        process_executor=ProcessPoolExecutor(max_workers=4),
        loop=asyncio.get_event_loop()
    )


async def main():
    start_time = time.time()
    session = set_session()
    set_paths()

    response = session["s3"].list_objects_v2(Bucket=config["bucket"], Prefix=config["prefix"], MaxKeys=1000)

    try:
        while response:
            next_response = session["thread_executor"].submit(session["s3"].list_objects_v2, Bucket=config["bucket"],
                                                              MaxKeys=100,
                                                              ContinuationToken=response["NextContinuationToken"])
            await download_and_zip(response, session)
            response = next_response.result()
    except:
        await download_and_zip(response, session)

    print("--- %s seconds ---" % (time.time() - start_time))


if __name__ == '__main__':
    asyncio.run(main())
