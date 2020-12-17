import argparse
import logging
import sys

import watchtower
from http_to_s3 import run_pipeline

if __name__ == "__main__":
    # Parse arguments
    argparser = argparse.ArgumentParser(description='''Download data from HTTP to S3 bucket.''',
                                        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                        conflict_handler='resolve')
    argparser.add_argument("--dataset-name",
                           help="List of dataset names to upload (d1 d2...)",
                           nargs="+")
    argparser.add_argument("--local-dir",
                           help="Local directory to store files before upload",
                           required=True)
    argparser.add_argument("--s3-bucket",
                           help="Destination S3 bucket",
                           required=True)
    argparser.add_argument("--s3-dir",
                           help="directory on S3 bucket",
                           required=True)
    argparser.add_argument("--chunk-size",
                           help="Chunk size (in megabytes) to use for splitting the files = max file size for transfer (the same for download and upload)",
                           required=False,
                           type=int,
                           default=250)
    argparser.add_argument("--logging-level",
                           help="Logging level",
                           required=False,
                           default="INFO",
                           choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    overwrite_parser = argparser.add_mutually_exclusive_group(required=False)
    overwrite_parser.add_argument(
        '--overwrite', dest='overwrite', action='store_true')
    overwrite_parser.add_argument(
        '--no-overwrite', dest='overwrite', action='store_false')
    argparser.set_defaults(overwrite=False)
    args = argparser.parse_args()

    # Logging configuration
    logging.basicConfig(level=getattr(logging, args.logging_level),
                        format='%(asctime)s - [%(levelname)s] - %(name)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger(__name__)
    logger.addHandler(watchtower.CloudWatchLogHandler(
        stream_name="http-to-s3"))

    # Run ETL: download files from HTTP and upload to S3
    try:
        run_pipeline(file_list=args.dataset_name,
                     intermediate_local=args.local_dir,
                     target_bucket=args.s3_bucket,
                     target_dir=args.s3_dir,
                     chunk_size=args.chunk_size,
                     overwrite=args.overwrite)
        logger.info("Finished successfully")
    except Exception as e:
        logger.exception(e)
        sys.exit(1)
