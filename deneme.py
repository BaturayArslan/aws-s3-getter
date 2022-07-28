import multiprocessing
import sys
import os
import asyncio
import functools
import time
import re
from io import BytesIO
import boto3
import ctypes
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from zipfile import ZipFile, ZIP_DEFLATED
import multiprocessing.sharedctypes
from pathlib import Path

from config import config


def set_paths() -> Path:
    cwd = os.getcwd()
    path = Path(cwd) / "s3-folder"
    path.mkdir(exist_ok=True)
    return path


def create_file(key: str) -> Path:
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

    return path


def check_response(response):
    if response["KeyCount"] == 0:
        sys.exit("Folder  NOT Found With this Prefix : {}".format(
            response["Prefix"]))


def check_key(key: str, config):
    if re.match(config["pattern"], key):
        return True
    else:
        return False


def zip_file(key,balancer,data):
    try:
        with ZipFile(f"s3-folder-{balancer}.zip", "a",ZIP_DEFLATED) as zip:
            zip.writestr(zinfo_or_arcname=key,data=data.value)
        return key
    except Exception as e:
        # TODO Learn how to display your custom error messsages with default one.
        message = f"While zipping file :: {key} an error occured. PID:: {multiprocessing.current_process().pid} "
        print(message)
        raise e


async def download_and_zip(response: dict, session: dict):
    def aio(f):
        async def io_wrapper(*args, **kwargs):
            bounded_f = functools.partial(f, *args, **kwargs)
            loop = session["loop"]
            return await loop.run_in_executor(session["thread_executor"], bounded_f)

        return io_wrapper

    get_object = aio(session["s3"].get_object)

    tasks = []
    for obj in response["Contents"]:
        if check_key(obj["Key"], config):
            path = create_file(obj["Key"])
            tasks.append(
                (session["loop"].create_task(get_object(Bucket=config["bucket"],Key=obj["Key"])), obj["Key"]))

    zip_processes = []
    bobin_counter = 0
    core_number = multiprocessing.cpu_count()
    for task, key in tasks:
        s3_response = await task
        print(f"{key} gonna zip")
        arr = bytes()
        for chunk in s3_response["Body"].iter_chunks():
            arr += chunk
        manager = multiprocessing.Manager()
        deneme = manager.Value(ctypes.c_char_p,arr,lock=False)
        # Simple Load Balancing Algorithm
        balancer = bobin_counter % core_number
        zip_processes.append(session["process_executor"].submit(zip_file,key,balancer,deneme))
        bobin_counter += 1

    for process in as_completed(zip_processes):
        key = process.result()
        print(f"Zipped :: {key}")

    with ZipFile("s3-folder.zip","a") as zip:
        for files in Path(os.getcwd()).rglob("s3-folder-?.zip"):
            zip.write(str(files))


def init(l):
    global lock
    lock = l

def set_session() -> dict:
    lock = multiprocessing.Lock()
    return dict(
        s3=boto3.client("s3"),
        start_time=time.time(),
        thread_executor=ThreadPoolExecutor(max_workers=10),
        lock = lock,
        process_executor=ProcessPoolExecutor(max_workers=round(multiprocessing.cpu_count()/2),initializer=init,initargs=(lock,)),
        loop=asyncio.get_event_loop()
    )


async def main():
    start_time = time.time()
    session = set_session()
    set_paths()

    response = session["s3"].list_objects_v2(Bucket=config["bucket"], Prefix=config["prefix"], MaxKeys=100)

    while response:
        try:
            next_response = session["thread_executor"].submit(session["s3"].list_objects_v2, Bucket=config["bucket"],
                                                              MaxKeys=1000,
                                                              ContinuationToken=response["NextContinuationToken"])
        except Exception as e:
            await download_and_zip(response, session)
            print("--- %s seconds ---" % (time.time() - start_time))
            sys.exit(0)

        await download_and_zip(response, session)
        response = next_response.result()


try:
    if __name__ == '__main__':
        asyncio.run(main())
except Exception as e:
    raise (e)
