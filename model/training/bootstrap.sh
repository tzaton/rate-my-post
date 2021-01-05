#!/bin/bash

# EMR bootstrap action

sudo mkdir /usr/lib/nltk_data
sudo python3 -m nltk.downloader -d /usr/lib/nltk_data punkt
sudo python3 -m pip install pyarrow==0.14 pandas==1.2.0

sudo yum install python3-devel -y
sudo python3 -m pip install cython
sudo python3 -m pip install mleap
