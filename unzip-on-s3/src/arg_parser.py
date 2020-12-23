import argparse


def get_parser():
    # Parse arguments
    argparser = argparse.ArgumentParser(description='''Unzip S3 data and upload to new directory''',
                                        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                        conflict_handler='resolve')
    argparser.add_argument("--dataset-name",
                           help="Dataset name (file key)",
                           required=True)
    argparser.add_argument("--local-dir",
                           help="Local directory to store files before upload",
                           required=True)
    argparser.add_argument("--s3-bucket",
                           help="Destination S3 bucket",
                           required=True)
    argparser.add_argument("--s3-dir",
                           help="output directory on S3 bucket",
                           required=True)
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
