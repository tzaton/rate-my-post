import argparse


def get_parser():
    # Parse arguments
    argparser = argparse.ArgumentParser(description='''Download data from HTTP to S3 bucket.''',
                                        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                        conflict_handler='resolve')
    argparser.add_argument("--dataset-name",
                           help="List of dataset names to upload (d1 d2...)",
                           nargs="+",
                           required=True)
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
                           default="250")
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
    return argparser
