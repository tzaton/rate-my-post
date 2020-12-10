import concurrent.futures
import json
import logging
import math
import os
import shutil
from pathlib import Path

import boto3
import botostubs
from botocore.exceptions import ClientError
import requests
import xmltodict
from tqdm import tqdm

# Load config
with open(Path(__file__).parent/ "config.json") as f:
    conf = json.load(f)

data_parent_url = conf["source_data"]["parent_url"]
metadata_file = data_parent_url + conf["source_data"]["metadata_file"]

logger = logging.getLogger(conf["source_data"]["logger"]["name"])

def get_file_size(file_name):
    file_url = data_parent_url + file_name
    with requests.head(file_url, allow_redirects=True) as r:
        r.raise_for_status()
        return int(r.headers["Content-Length"])


def download_file(source_name, destination_name, destination_dir, headers=None):
    file_url = data_parent_url + source_name
    file_location = os.path.join(destination_dir, destination_name)

    with requests.get(file_url, headers=headers, allow_redirects=True, stream=True) as r:
        r.raise_for_status()
        with open(file_location, 'wb') as f:
            shutil.copyfileobj(r.raw, f, length=1024 * 1024 * 10)
    logger.debug("Downloaded: %s to: %s", r.url, file_location)
    return file_location


def get_file_list():
    with requests.get(metadata_file, allow_redirects=True) as r:
        r.raise_for_status()
        return [x["@name"] for x in xmltodict.parse(r.content)["files"]["file"]
                if x["@name"].endswith("7z")]


def upload_file(path, bucket, key, overwrite):
    s3: botostubs.S3 = boto3.client('s3')
    upload_flag = True
    if overwrite is False:
        try:
            s3.head_object(Key=key, Bucket=bucket)
            logger.debug("Object already exists: /%s/%s. Skipping because `overwrite`=False was specified", bucket, key)
            upload_flag = False
        except ClientError:
            pass
    if upload_flag is True:
        s3.upload_file(path, Bucket=bucket, Key=key)
        logger.debug("File: %s uploaded to: /%s/%s", path, bucket, key)

def remove_file(path):
    os.remove(path)
    logger.debug("Deleted: %s", path)


def process_file(source_name, file_name, download_dir, upload_bucket, upload_dir,
                 overwrite, headers=None):
    local_file = download_file(source_name, file_name, download_dir, headers)
    upload_file(local_file, upload_bucket, os.path.join(upload_dir, file_name),
                overwrite)
    remove_file(local_file)


def run_pipeline(file_list,
                 intermediate_local,
                 target_bucket,
                 chunk_size,
                 overwrite):
    # Calculate file size
    total_size = 0
    file_size_dict = dict()
    n_files = len(file_list)

    logger.info("Calculating total file size...")
    logger.info("Number of files: %s", n_files)
    with tqdm(total=n_files) as pbar:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = {}
            for file_name in file_list:
                size_check = executor.submit(get_file_size, file_name)
                futures[size_check] = file_name
            for future in concurrent.futures.as_completed(futures):
                file_size_dict[futures[future]] = future.result()
                total_size += future.result()
                pbar.update(1)
    logger.info("Total file size is: ~%s MB", round(total_size/1024/1024))
    n_parts_dict = {file_name: math.ceil(file_size_dict[file_name]/chunk_size)
                    for file_name in file_size_dict}
    logger.info("Number of parts to be created: %s", sum(n_parts_dict.values()))

    # Download -> upload -> remove
    logger.info(f"Downloading files and uploading to S3...")
    with tqdm(total=total_size, unit="B") as pbar:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = {}
            for f in file_list:
                size = file_size_dict[f]
                if size > chunk_size:
                    n_parts = n_parts_dict[f]
                    start = 0
                    part_number = 1
                    while start <= size:
                        end = min(start + chunk_size, size)
                        part_file_name = f"{f.split('.7z')[0]}_part{part_number}of{n_parts}.7z"
                        process_thread = executor.submit(process_file,
                                                        source_name=f,
                                                        file_name=part_file_name,
                                                        download_dir=intermediate_local,
                                                        upload_bucket=target_bucket,
                                                        upload_dir="raw",
                                                        overwrite=overwrite,
                                                        headers={"Range": f"bytes={start}-{end}"})
                        if start == 0:
                            futures[process_thread] = end - start
                        else:
                            futures[process_thread] = end - start + 1
                        start = end + 1
                        part_number += 1
                else:
                    process_thread = executor.submit(process_file,
                                                    source_name=f,
                                                    file_name=f,
                                                    download_dir=intermediate_local,
                                                    upload_bucket=target_bucket,
                                                    upload_dir="raw",
                                                    overwrite=overwrite)
                    futures[process_thread] = file_size_dict[f]

            for future in concurrent.futures.as_completed(futures):
                pbar.update(futures[future])
