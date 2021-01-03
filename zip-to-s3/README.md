# zip-to-s3
Download 7z-compressed data from HTTP to S3

<<<<<<< HEAD
```bash
# Build and push docker images
bash zip-to-s3/docker-build.sh
bash unzip-on-s3/docker-build.sh

# Run data import
=======
```
bash zip-to-s3/docker-build.sh
bash unzip-on-s3/docker-build.sh

>>>>>>> cf0eb2526f62045d460c453fdd7a12977f0676e3
python zip-to-s3/ecs_run.py \
--parent-url https://archive.org/download/stackexchange/ \
--local-dir /tmp \
--s3-bucket rate-my-post \
--s3-dir zip \
<<<<<<< HEAD
--dataset-name $(cat zip-to-s3/datasets.txt) \
=======
--dataset-name $(cat zip-to-s3/test_data.txt) \
>>>>>>> cf0eb2526f62045d460c453fdd7a12977f0676e3
--overwrite
```
