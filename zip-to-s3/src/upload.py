import concurrent.futures
import logging
import math
import os
import re
import shutil
from pathlib import Path

import boto3
import requests
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

DATA_PARENT_URL = os.environ["DATA_PARENT_URL"]
MAX_WORKERS = 10


def get_url_size(file_name):
    file_url = DATA_PARENT_URL + file_name
    logger.info("Getting file size: %s", file_url)
    with requests.head(file_url, allow_redirects=True) as r:
        r.raise_for_status()
        return int(r.headers["Content-Length"])


def download_file(source_name, destination_name, destination_dir, **kwargs):
    file_url = DATA_PARENT_URL + source_name
    logger.info("Downloading: %s", file_url)
    file_location = os.path.join(destination_dir, destination_name)

    with requests.get(file_url, allow_redirects=True, stream=True, **kwargs) as r:
        r.raise_for_status()
        with open(file_location, 'wb') as f:
            shutil.copyfileobj(r.raw, f, length=1024*1024*10)
    logger.info("Downloaded: %s to: %s", r.url, file_location)
    return file_location


def check_file_on_s3(s3_client, bucket, key):
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        logger.info(
            "Object exists on S3: %s/%s", bucket, key)
        return True
    except ClientError:
        logger.info(
            "Object doesn't exist on S3: %s/%s", bucket, key)
        return False


def remove_file(path):
    os.remove(path)
    logger.info("Deleted: %s", path)


def concatenate_parts(file_name, directory, parts_list, remove=True):
    parent_file_path = os.path.join(directory, file_name)
    with open(parent_file_path, "wb") as output_file:
        parts = [(p, re.search(r"_part(\d+)", p).groups(1)[0])
                 for p in parts_list]
        parts.sort(key=lambda x: int(x[1]))
        for part in parts:
            part_file_path = os.path.join(directory, part[0])
            with open(part_file_path, "rb") as input_file:
                shutil.copyfileobj(input_file, output_file)
            if remove is True:
                remove_file(part_file_path)
        logger.info("Created: %s", parent_file_path)
    return parent_file_path


def get_file_size(path):
    return Path(path).stat().st_size


def upload_file(s3_client, path, bucket, key, **kwargs):
    logger.info("Uploading: %s to: %s/%s", path, bucket, key)
    s3_client.upload_file(path,
                          Bucket=bucket,
                          Key=key,
                          **kwargs)
    logger.info("File: %s uploaded to: %s/%s (%s MB)",
                path, bucket, key, round(get_file_size(path)/1024/1024, 1))


def run_pipeline(file_list,
                 intermediate_local,
                 target_bucket,
                 target_dir,
                 chunk_size,
                 overwrite):
    chunk_size = chunk_size*1024*1024  # convert to bytes
    # AWS
    sts = boto3.client('sts')
    sts.get_caller_identity()  # check credentials
    s3 = boto3.client('s3')
    transfer_config = TransferConfig(multipart_chunksize=chunk_size)

    # Calculate file size
    total_size = 0
    file_size_dict = dict()
    n_files = len(file_list)

    logger.info("Calculating total file size...")
    logger.info("Number of files: %s", n_files)
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {}
        for file_name in file_list:
            size_check = executor.submit(get_url_size, file_name)
            futures[size_check] = file_name
        for future in concurrent.futures.as_completed(futures):
            file_size_dict[futures[future]] = future.result()
            total_size += future.result()
    logger.info("Total file size is: %s MB", round(total_size/1024/1024, 1))
    n_parts_dict = {file_name: math.ceil(file_size_dict[file_name]/chunk_size)
                    for file_name in file_size_dict}
    logger.info("Number of parts to be created: %s",
                sum(n_parts_dict.values()))

    # Download -> concatenate (if needed) -> upload -> remove
    Path(intermediate_local).mkdir(parents=True, exist_ok=True)
    for f in file_list:
        s3_key = os.path.join(target_dir, f)
        if overwrite is False:
            if check_file_on_s3(s3_client=s3,
                                bucket=target_bucket,
                                key=s3_key) is True:
                logger.info(
                    "Skipping: %s because `overwrite` = False was specified", f)
                continue

        size = file_size_dict[f]
        if size > chunk_size:
            n_parts = n_parts_dict[f]
            start = 0
            part_number = 1
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {}
                while start <= size:
                    end = min(start + chunk_size, size)
                    part_file_name = f"{f.split('.7z')[0]}_part{part_number}of{n_parts}.7z"
                    part_download = executor.submit(download_file,
                                                    source_name=f,
                                                    destination_name=part_file_name,
                                                    destination_dir=intermediate_local,
                                                    headers={"Range": f"bytes={start}-{end}"})
                    futures[part_download] = part_file_name
                    start = end + 1
                    part_number += 1
            file = concatenate_parts(file_name=f,
                                     directory=intermediate_local,
                                     parts_list=list(futures.values()),
                                     remove=True)
        else:
            file = download_file(source_name=f,
                                 destination_name=f,
                                 destination_dir=intermediate_local)
        upload_file(s3_client=s3,
                    path=file,
                    bucket=target_bucket,
                    key=s3_key,
                    Config=transfer_config)
        remove_file(file)
