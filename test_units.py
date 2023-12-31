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

    with smart_open.open('test_data/test1.zip', 'rb', transport_params=dict(client=None)) as f:
        files_in_zip_main = read_central_dir(f)

    assert list_files('test_data/test1.zip', files_in_zip_main) == '''Archive:  test_data/test1.zip
  Length      Date    Time    Name
---------  ---------- -----   ----
        0  2023-12-31 12:51   empty.txt
---------                     ----
        0                     1 files'''


