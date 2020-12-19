import logging
import sys

from upload import run_pipeline
from arg_parser import get_parser


if __name__ == "__main__":
    argparser = get_parser()
    args = argparser.parse_args()
    if len(args.dataset_name) == 1:
        args.dataset_name = [d.split(" ") for d in args.dataset_name][0]

    # Logging configuration
    logging.basicConfig(level=getattr(logging, args.logging_level),
                        format='%(asctime)s - [%(levelname)s] - %(name)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger(__name__)

    # Run ETL: download files from HTTP and upload to S3
    try:
        run_pipeline(file_list=args.dataset_name,
                     intermediate_local=args.local_dir,
                     target_bucket=args.s3_bucket,
                     target_dir=args.s3_dir,
                     chunk_size=int(args.chunk_size),
                     overwrite=args.overwrite)
        logger.info("Finished successfully")
    except Exception as e:
        logger.exception(e)
        sys.exit(1)
