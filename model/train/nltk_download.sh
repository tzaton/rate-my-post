#!/bin/bash

# EMR bootstrap action - download NLTK
sudo mkdir /usr/lib/nltk_data
sudo python3 -m nltk.downloader -d /usr/lib/nltk_data stopwords punkt averaged_perceptron_tagger universal_tagset vader_lexicon
