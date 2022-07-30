import multiprocessing
import sys
import os
import time
import re
import boto3
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from threading import Lock
from zipfile import ZipFile, ZIP_DEFLATED
import multiprocessing.sharedctypes
from pathlib import Path
from random import randrange
from ctypes import c_int

from config import config


def set_paths() -> Path:
    cwd = os.getcwd()
    path = Path(cwd) / "s3-folder"
    path.mkdir(exist_ok=True)
    return path

def clean_path():
    os.rmdir(Path(os.getcwd()).joinpath("s3-folder"))


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


def zip_file(key, file_path, pid):
    try:
        with ZipFile(f"s3-folder-{pid}.zip", "a", ZIP_DEFLATED) as zip:
            zip.write(filename=file_path, arcname=key)
    except Exception as e:
        # TODO Learn how to display your custom error messsages with default one.
        message = f"While zipping file :: {key} an error occured. PID:: {multiprocessing.current_process().pid} "
        print(message)
        raise Exception(e.args,message).with_traceback(e.__traceback__)



def download_object(object, client, lock):
    file_name = object["Key"].split("/")[-1]
    suffix = "." + file_name.split(".")[-1]
    tmp_file_path = str(Path(os.getcwd()).joinpath("s3-folder",f"{randrange(10000)}{suffix}"))
    pid = multiprocessing.current_process().pid
    client.download_file(config["bucket"], object["Key"], tmp_file_path)
    with lock:
        print(f"{object['Key']} gonna zip")
        zip_file(object["Key"], tmp_file_path, pid)
        print(f"{object['Key']}:: ZIPPED!")
    os.remove(tmp_file_path)

def download_and_zip(objects):
    with ThreadPoolExecutor(max_workers=6) as executor:
        client = boto3.client("s3")
        lock = Lock()
        futures = []
        for object in objects:
            if check_key(object["Key"], config):
                futures.append(executor.submit(download_object, object, client, lock))

        for future in as_completed(futures):
            COUNTER.value += 1
            future.result()


def init(counter):
    global COUNTER
    COUNTER = counter

def set_session() -> dict:
    download_counter = multiprocessing.Value(c_int,1)
    key_counter = 0
    return dict(
        s3=boto3.client("s3"),
        thread_executor=ThreadPoolExecutor(max_workers=2),
        process_executor=ProcessPoolExecutor(max_workers=round(multiprocessing.cpu_count() / 2),initializer=init,initargs=(download_counter,)),
        download_counter=download_counter,
        key_counter=key_counter
    )


def processes_init(response, session):
    i = 0
    arr = []
    content_arr = response["Contents"]
    step = round(len(content_arr) / round(multiprocessing.cpu_count() / 2))
    remainder = len(content_arr) % round(multiprocessing.cpu_count() / 2)

    while i < len(content_arr):
        if i + step >= len(content_arr) - remainder:
            arr.append(content_arr[i:])
            break
        else:
            arr.append(content_arr[i:i + step])
        i += step

    futures = [session["process_executor"].submit(download_and_zip, chunks) for chunks in arr]
    for future in as_completed(futures):
        future.result()

    print("Zipped part Assembling..".center(60,"-"))
    with ZipFile("s3-folder.zip","w") as zip:
        for files in Path(os.getcwd()).rglob("s3-folder-*.zip"):
            zip.write(filename=files,arcname=files.relative_to(Path(os.getcwd())))

    session["key_counter"] += len(response["Contents"])


def main():
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
            processes_init(response, session)
            print(f"Downloaded and Zipped {session['download_counter'].value}/{session['key_counter']}")
            print("--- %s seconds ---" % (time.time() - start_time))
            clean_path()
            sys.exit(0)

        processes_init(response, session)
        response = next_response.result()

try:
    if __name__ == '__main__':
        main()
except Exception as e:
    raise (e)
