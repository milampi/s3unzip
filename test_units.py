#!/usr/bin/python3 -B
# -*- coding: utf-8 -*-

import pytest
import os

from s3unzip import *

# Fixtures ------------------------------------------------------------------

@pytest.fixture
def local_file_name():
    '''Fixture to return local filesystem url to zip file'''
    return 'test_data/test1.zip'


@pytest.fixture
def local_transport_client():
    '''Fixture to return local filesystem i.e. empty (None) transport client'''
    return None


@pytest.fixture
def s3_file_name():
    '''Fixture to return transport client for s3 based zip file'''
    return 's3://testbase/s3unzip_testing/test1.zip'


@pytest.fixture
def s3_transport_client():
    '''Fixture to return transport client for s3 based zip file'''
    # Reads in config used by s3cmd and uses the login credentials from it.
    # s3cmd config is usually in file ~/.s3cfg
    config = configparser.ConfigParser()
    with open(f'{os.getenv("HOME")}/.s3cfg_scaleway') as f:
        config.read_file(f)
    client = create_transport_client(config)

    return client

# Local zip file tests ------------------------------------------------------

def test_find_central_dir_direct_open():
    '''Test to see we are still compatible with direct access filesystem'''
    with open('test_data/test1.zip','rb') as f:
        assert find_central_dir(f) == 67


def test_find_central_dir(local_file_name, local_transport_client):
    with smart_open.open(local_file_name, 'rb', transport_params=dict(client=local_transport_client)) as f:
        assert find_central_dir(f) == 67


def test_read_central_dir(local_file_name, local_transport_client):
    with smart_open.open(local_file_name, 'rb', transport_params=dict(client=local_transport_client)) as f:
        files_in_zip = read_central_dir(f)
    assert files_in_zip == {'empty.txt': {'date_time': datetime.datetime(2023, 12, 31, 12, 51, 2), 'length': 0, 'position': 0}}


def test_parse_central_dir_file_header():
    file_header = b'\x1e\x03\n\x00\x00\x00\x00\x00af\x9fW\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\t\x00\x18\x00\x00\x00\x00\x00\x00\x00\x00\x00\xa4\x81\x00\x00\x00\x00empty.txtUT\x05\x00\x03\x96G\x91eux\x0b\x00\x01\x04\xe8\x03\x00\x00\x04d\x00\x00\x00'

    result = parse_central_dir_file_header(io.BytesIO(file_header))

    correct_result = {'ver': 798, 'minver': 10, 'flags': 0, 'method': 0, 'time': 26209, 'date': 22431, 'crc32': 0, 'comp_len': 0, 'uncomp_len': 0, 'fname_len': 9, 'extra_len': 24, 'comment_len': 0, 'disk_num': 0, 'iattr': 0, 'eattr': 2175008768, 'start': 0, 'fname': 'empty.txt', 'date_time': datetime.datetime(2023, 12, 31, 12, 51, 2), 'extra': {21589: {'mod_time': datetime.datetime(2023, 12, 31, 10, 51, 2)}, 30837: {'version': 1, 'uid': 1000, 'gid': 3}}, 'comment': ''}

    assert result == correct_result


def test_parse_endof_central_dir_record():
    eocd_record = b'\x00\x00\x00\x00\x01\x00\x01\x00O\x00\x00\x00C\x00\x00\x00\x00\x00'

    result = parse_endof_central_dir_record(io.BytesIO(eocd_record))

    correct_result = {'disk_num': 0, 'dir_disk_num': 0, 'disk_records_num': 1, 'tot_records_num': 1, 'dir_len': 79, 'dir_start': 67, 'comment_len': 0, 'comment': ''}

    assert result == correct_result


def test_parse_extra_fields():
    extra_field = b'UT\x05\x00\x03\x96G\x91eux\x0b\x00\x01\x04\xe8\x03\x00\x00\x04d\x00\x00\x00'

    result = parse_extra_fields(extra_field)

    correct_result = {21589: {'mod_time': datetime.datetime(2023, 12, 31, 10, 51, 2)}, 30837: {'version': 1, 'uid': 1000, 'gid': 3}}

    assert result == correct_result


def test_pretty_print_files(local_file_name, local_transport_client):
    with smart_open.open(local_file_name, 'rb', transport_params=dict(client=local_transport_client)) as f:
        files_in_zip = read_central_dir(f)

    # The aim is to resemble normal zip command output as closely as possible
    pretty_print_target = '''Archive:  test_data/test1.zip
  Length      Date    Time    Name
---------  ---------- -----   ----
        0  2023-12-31 12:51   empty.txt
---------                     ----
        0                     1 files'''

    assert '\n'.join(pretty_print_files('test_data/test1.zip', files_in_zip)) == pretty_print_target


def test_unzip_file_at_pos():
    os.chdir('test_data')
    unzip_file_at_pos('test1.zip', 'empty.txt', 0, None)

    assert os.path.isfile('empty.txt')

    os.remove('empty.txt') 
    os.chdir('..')


# S3 based zip file tests ---------------------------------------------------


@pytest.mark.skipif(not os.path.isfile(f'{os.getenv("HOME")}/.s3cfg_scaleway'), reason="No scaleway credientials defined")
def test_s3_create_transport_client():
    '''Test if creation of transport credientials works'''
    config = configparser.ConfigParser()
    with open(f'{os.getenv("HOME")}/.s3cfg_scaleway') as f:
        config.read_file(f)
    client = create_transport_client(config)

    # Not much we can test here
    assert client._endpoint._endpoint_prefix == 's3'


@pytest.mark.skipif(not os.path.isfile(f'{os.getenv("HOME")}/.s3cfg_scaleway'), reason="No scaleway credientials defined")
def test_s3_read_central_dir(s3_file_name, s3_transport_client):
    with smart_open.open(s3_file_name, 'rb', transport_params=dict(client=s3_transport_client)) as f:
        files_in_zip = read_central_dir(f)
    assert files_in_zip == {'empty.txt': {'date_time': datetime.datetime(2023, 12, 31, 12, 51, 2), 'length': 0, 'position': 0}}


