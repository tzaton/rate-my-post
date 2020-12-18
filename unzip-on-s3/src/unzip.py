import shutil
import logging
import os
from pathlib import Path

import boto3
import botostubs
import stringcase
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError
from py7zr import unpack_7zarchive


logger = logging.getLogger(__name__)


def get_file_size(path):
    return Path(path).stat().st_size


def get_dir_size(path):
    return sum([get_file_size(p) for p in Path(path).rglob("*")])


def download_file(s3_client, bucket, key, path, **kwargs):
    logger.info("Downloading: %s/%s to: %s", bucket, key, path)
    s3_client.download_file(Filename=path,
                            Bucket=bucket,
                            Key=key,
                            **kwargs)
    logger.info("Downloaded: %s/%s to: %s", bucket, key, path)
    return path


def remove_file(path):
    os.remove(path)
    logger.info("Deleted: %s", path)


def unzip_file(zip_path, unzip_path, remove=True):
    zip_size = round(get_file_size(zip_path)/1024/1024, 1)
    shutil.unpack_archive(zip_path, unzip_path)
    unzip_size = round(get_dir_size(unzip_path)/1024/1024, 1)
    logger.info("File: %s unzipped to: %s (%s MB -> %s MB)",
                zip_path, unzip_path, zip_size, unzip_size)
    if remove is True:
        remove_file(zip_path)
    return unzip_path


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


def upload_file(s3_client, path, bucket, key, overwrite, **kwargs):
    upload_flag = True
    if overwrite is False:
        if check_file_on_s3(s3_client, bucket, key) is True:
            logger.info(
                "Skipping: %s because `overwrite` = False was specified", key)
            upload_flag = False
    if upload_flag is True:
        logger.info("Uploading: %s to: %s/%s", path, bucket, key)
        s3_client.upload_file(path,
                              Bucket=bucket,
                              Key=key,
                              **kwargs)
        logger.info("File: %s uploaded to: %s/%s (%s MB)",
                    path, bucket, key, round(get_file_size(path)/1024/1024, 1))


def upload_folder(s3_client, in_path, bucket, out_dir, overwrite, **kwargs):
    logger.info("Uploading files from directory: %s", in_path)
    for f in Path(in_path).glob("*"):
        s3_folder = stringcase.snakecase(os.path.splitext(f.name)[0])
        s3_filename = f.parent.name.lower().replace(
            ".", "-") + os.path.splitext(f.name)[1]
        s3_path = os.path.join(out_dir, s3_folder, s3_filename)
        upload_file(s3_client=s3_client,
                    path=str(f),
                    bucket=bucket,
                    key=s3_path,
                    overwrite=overwrite,
                    **kwargs)


def remove_directory(path):
    shutil.rmtree(path)
    logger.info("Deleted: %s", path)


def run_pipeline(file_name,
                 local,
                 bucket,
                 target_dir,
                 chunk_size,
                 overwrite):
    chunk_size = chunk_size*1024*1024  # convert to bytes
    # AWS
    sts: botostubs.STS = boto3.client('sts')
    sts.get_caller_identity()  # check credentials
    s3: botostubs.S3 = boto3.client('s3')
    transfer_config = TransferConfig(multipart_chunksize=chunk_size)
    # 7zip
    shutil.register_unpack_format('7zip', ['.7z'], unpack_7zarchive)

    Path(local).mkdir(parents=True, exist_ok=True)
    file = download_file(s3_client=s3,
                         bucket=bucket,
                         key=file_name,
                         path=os.path.join(local, os.path.basename(file_name)))
    file_unzipped = unzip_file(zip_path=file,
                               unzip_path=os.path.basename(
                                   file_name).split('.7z')[0],
                               remove=True)
    upload_folder(s3_client=s3,
                  in_path=file_unzipped,
                  bucket=bucket,
                  out_dir=target_dir,
                  overwrite=overwrite,
                  Config=transfer_config)
    remove_directory(file_unzipped)
