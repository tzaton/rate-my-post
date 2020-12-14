import concurrent.futures
import logging
import math
import os
import shutil
import re

import boto3
import botostubs
import requests
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig
from py7zr import unpack_7zarchive


logger = logging.getLogger(__name__)
data_parent_url = os.environ["DATA_PARENT_URL"]


def get_file_size(file_name):
    file_url = data_parent_url + file_name
    with requests.head(file_url, allow_redirects=True) as r:
        r.raise_for_status()
        return int(r.headers["Content-Length"])


def download_file(source_name, destination_name, destination_dir, **kwargs):
    file_url = data_parent_url + source_name
    file_location = os.path.join(destination_dir, destination_name)

    with requests.get(file_url, allow_redirects=True, stream=True, **kwargs) as r:
        r.raise_for_status()
        with open(file_location, 'wb') as f:
            shutil.copyfileobj(r.raw, f, length=1024*1024*10)
    logger.info("Downloaded: %s to: %s", r.url, file_location)
    return file_location


def upload_file(s3_client, path, bucket, key, overwrite, **kwargs):
    upload_flag = True
    if overwrite is False:
        try:
            s3_client.head_object(Key=key, Bucket=bucket)
            logger.info(
                "Object already exists: /%s/%s. Skipping because `overwrite`=False was specified", bucket, key)
            upload_flag = False
        except ClientError:
            pass
    if upload_flag is True:
        s3_client.upload_file(path,
                              Bucket=bucket,
                              Key=key,
                              **kwargs)
        logger.info("File: %s uploaded to: /%s/%s", path, bucket, key)


def upload_folder(s3_client, in_path, bucket, out_path, overwrite, **kwargs):
    logger.info("Uploading directory: %s", in_path)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for root, _, files in os.walk(in_path):
            for f in files:
                local_path = os.path.join(root, f)
                relative_path = os.path.relpath(local_path, in_path)
                s3_path = os.path.join(out_path, relative_path)
                executor.submit(upload_file,
                                s3_client=s3_client,
                                path=local_path,
                                bucket=bucket,
                                key=s3_path,
                                overwrite=overwrite,
                                **kwargs)


def remove_file(path):
    os.remove(path)
    logger.info("Deleted: %s", path)


def remove_directory(path):
    shutil.rmtree(path)
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


def unzip_file(zip_path, unzip_path, remove=True):
    shutil.unpack_archive(zip_path, unzip_path)
    logger.info("File: %s unzipped to: %s", zip_path, unzip_path)
    if remove is True:
        remove_file(zip_path)
    return unzip_path


def run_pipeline(file_list,
                 intermediate_local,
                 target_bucket,
                 chunk_size,
                 overwrite):
    chunk_size = chunk_size*1024*1024  # convert to bytes
    # S3
    s3: botostubs.S3 = boto3.client('s3')
    upload_dir = "raw"
    transfer_config = TransferConfig(multipart_chunksize=chunk_size)

    # 7zip
    shutil.register_unpack_format('7zip', ['.7z'], unpack_7zarchive)

    # Calculate file size
    total_size = 0
    file_size_dict = dict()
    n_files = len(file_list)

    logger.info("Calculating total file size...")
    logger.info("Number of files: %s", n_files)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {}
        for file_name in file_list:
            size_check = executor.submit(get_file_size, file_name)
            futures[size_check] = file_name
        for future in concurrent.futures.as_completed(futures):
            file_size_dict[futures[future]] = future.result()
            total_size += future.result()
    logger.info("Total file size is: ~%s MB", round(total_size/1024/1024))
    n_parts_dict = {file_name: math.ceil(file_size_dict[file_name]/chunk_size)
                    for file_name in file_size_dict}
    logger.info("Number of parts to be created: %s",
                sum(n_parts_dict.values()))

    # Download -> concatenate (if needed) -> zip -> upload -> remove
    for f in file_list:
        size = file_size_dict[f]
        if size > chunk_size:
            n_parts = n_parts_dict[f]
            start = 0
            part_number = 1
            with concurrent.futures.ThreadPoolExecutor() as executor:
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

        file_unzipped = unzip_file(zip_path=file,
                                   unzip_path=os.path.join(
                                       intermediate_local, f.split(".7z")[0]),
                                   remove=True)
        upload_folder(s3_client=s3,
                      in_path=file_unzipped,
                      bucket=target_bucket,
                      out_path=os.path.join(upload_dir, f.split(".7z")[0]),
                      overwrite=overwrite,
                      Config=transfer_config)
        remove_directory(file_unzipped)
