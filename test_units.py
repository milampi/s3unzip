#!/usr/bin/python3 -B
# -*- coding: utf-8 -*-

import pytest
#import imp
#from pytest import approx

from s3unzip import *

def test_find_central_dir():
    with open('test_data/test1.zip','rb') as f:
        assert find_central_dir(f) == 67

# TODO def test_read_central_dir():

def test_list_files():
    config = configparser.ConfigParser()
    # TODO convert to work without external dependency of .s3cfg file
    with open(f'{os.getenv("HOME")}/.s3cfg') as f: config.read_file(f)
    client = create_tranport_client(config)

    with smart_open.open('test_data/test1.zip', 'rb', transport_params=dict(client=client)) as f:
        files_in_zip_main = read_central_dir(f)

    assert list_files(files_in_zip_main) == '''  Length      Date    Time    Name
---------  ---------- -----   ----
        0  2023-12-31 12:51   empty.txt
---------                     ----
        0                     1 files'''


