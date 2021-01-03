# zip-to-s3
Download 7z-compressed data from HTTP to S3

```bash
# Build and push docker images
bash zip-to-s3/docker-build.sh
bash unzip-on-s3/docker-build.sh

# Run data import
python zip-to-s3/ecs_run.py \
--parent-url https://archive.org/download/stackexchange/ \
--local-dir /tmp \
--s3-bucket rate-my-post \
--s3-dir zip \
--dataset-name $(cat zip-to-s3/datasets.txt) \
--overwrite
```
