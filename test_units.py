#!/usr/bin/python3 -B
# -*- coding: utf-8 -*-

import pytest
#import imp
#from pytest import approx

from s3unzip import *

def test_find_central_dir():
    with open('test_data/test1.zip','rb') as f:
        assert find_central_dir(f) == 67

# TODO def test_read_central_dir(s3_stream: io.BufferedIOBase) -> dict:

def test_pretty_print_files():
    config = configparser.ConfigParser()

    with smart_open.open('test_data/test1.zip', 'rb', transport_params=dict(client=None)) as f:
        files_in_zip_main = read_central_dir(f)

    assert '\n'.join(pretty_print_files('test_data/test1.zip', files_in_zip_main)) == '''Archive:  test_data/test1.zip
  Length      Date    Time    Name
---------  ---------- -----   ----
        0  2023-12-31 12:51   empty.txt
---------                     ----
        0                     1 files'''


'''
TODO
def _extra_field_universal_time(hlen: int, hbytes: bytes, in_cd: bool) -> dict:
def _extra_field_uid_gid(hbytes: bytes) -> dict:
def parse_extra_fields(extra_bytes: bytes, in_cd: bool = True) -> dict:
def parse_local_file_header(s3_stream: io.BufferedIOBase) -> dict:
def parse_central_dir_file_header(s3_stream: io.BufferedIOBase) -> dict:
def parse_endof_central_dir_record(s3_stream: io.BufferedIOBase) -> dict:
def unzip_file_at_pos(s3_file_name: str, out_file_name: str, pos: int, client: Any) -> None:
def create_tranport_client(config: configparser.ConfigParser) -> botocore.client.BaseClient:
'''

