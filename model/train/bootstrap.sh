#!/bin/bash

# EMR bootstrap action
sudo mkdir /usr/lib/nltk_data
sudo python3 -m nltk.downloader -d /usr/lib/nltk_data punkt averaged_perceptron_tagger universal_tagset vader_lexicon
sudo python3 -m pip install pyarrow