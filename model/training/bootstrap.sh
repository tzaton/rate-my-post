#!/bin/bash

# EMR bootstrap action
sudo python3 -m pip install pyarrow==0.14 pandas==1.2.0 spark-nlp==2.6.2 matplotlib

sudo yum install python3-devel -y
sudo python3 -m pip install cython
sudo python3 -m pip install mleap boto3
