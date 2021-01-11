# zip-to-s3
Download 7z-compressed data from HTTP to S3

```bash
# Build and push docker images
bash data-import/zip-to-s3/docker-build.sh
bash data-import/unzip-on-s3/docker-build.sh

# Run data import
python data-import/zip-to-s3/ecs_run.py \
    --parent-url https://archive.org/download/stackexchange/ \
    --local-dir /tmp \
    --s3-bucket ratemypost \
    --s3-dir zip \
    --dataset-name $(cat data-import/datasets.txt) \
    --overwrite
```
