#!/usr/bin/python3 -B
# -*- coding: utf-8 -*-

import pytest
import os
import hashlib

from s3unzip import *

# Parameters ----------------------------------------------------------------

def scaleway_transport_client():
    '''Return transport client for s3 based zip file in scaleway'''
    # Reads in config used by s3cmd and uses the login credentials from it.
    # s3cmd config is usually in file ~/.s3cfg
    config = configparser.ConfigParser()
    with open(f'{os.getenv("HOME")}/.s3cfg_scaleway') as f:
        config.read_file(f)
    client = create_transport_client(config)

    return client

# List of different test targets for empty zip tests
testdata = [
    pytest.param( 'test_data/test1.zip', None, id="local"),
]

# If scaleway credientials exist, add them to test list
if os.path.isfile(f'{os.getenv("HOME")}/.s3cfg_scaleway'):
   testdata.append( pytest.param( 's3://testbase/s3unzip_testing/test1.zip', scaleway_transport_client(), id="scaleway") )

# List of different test targets for small zip tests
test2data = [
    pytest.param( 'test_data/test2.zip', None, id="local"),
]

# If scaleway credientials exist, add them to test list
if os.path.isfile(f'{os.getenv("HOME")}/.s3cfg_scaleway'):
   test2data.append( pytest.param( 's3://testbase/s3unzip_testing/test2.zip', scaleway_transport_client(), id="scaleway") )

# List of different test targets for small zip tests
test3data = [
    pytest.param( 'test_data/test3.zip', None, id="local"),
]

# If scaleway credientials exist, add them to test list
if os.path.isfile(f'{os.getenv("HOME")}/.s3cfg_scaleway'):
   test3data.append( pytest.param( 's3://testbase/s3unzip_testing/test3.zip', scaleway_transport_client(), id="scaleway") )


# Direct file tests ---------------------------------------------------------

def test_find_central_dir_direct_open():
    '''Test to see we are still compatible with direct access filesystem'''
    with open('test_data/test1.zip','rb') as f:
        assert find_central_dir(f) == 67


# Tests that don't need a connection ----------------------------------------

def test_parse_central_dir_file_header():
    file_header = b'\x1e\x03\n\x00\x00\x00\x00\x00af\x9fW\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'\
                  b'\x00\x00\t\x00\x18\x00\x00\x00\x00\x00\x00\x00\x00\x00\xa4\x81\x00\x00\x00'\
                  b'\x00empty.txtUT\x05\x00\x03\x96G\x91eux\x0b\x00\x01\x04\xe8\x03\x00\x00\x04d\x00\x00\x00'

    result = parse_central_dir_file_header(io.BytesIO(file_header))

    correct_result = {'ver': 798, 'minver': 10, 'flags': 0, 'method': 0, 'time': 26209, 'date': 22431,
                      'crc32': 0, 'comp_len': 0, 'uncomp_len': 0, 'fname_len': 9, 'extra_len': 24,
                      'comment_len': 0, 'disk_num': 0, 'iattr': 0, 'eattr': 2175008768, 'start': 0,
                      'fname': 'empty.txt', 'date_time': datetime.datetime(2023, 12, 31, 12, 51, 2),
                      'extra': {21589: {'mod_time': datetime.datetime(2023, 12, 31, 10, 51, 2)},
                                30837: {'version': 1, 'uid': 1000, 'gid': 3}}, 'comment': ''}

    assert result == correct_result


def test_parse_endof_central_dir_record():
    eocd_record = b'\x00\x00\x00\x00\x01\x00\x01\x00O\x00\x00\x00C\x00\x00\x00\x00\x00'

    result = parse_endof_central_dir_record(io.BytesIO(eocd_record))

    correct_result = {'disk_num': 0, 'dir_disk_num': 0, 'disk_records_num': 1, 'tot_records_num': 1,
                      'dir_len': 79, 'dir_start': 67, 'comment_len': 0, 'comment': ''}

    assert result == correct_result


def test_parse_extra_fields():
    extra_field = b'UT\x05\x00\x03\x96G\x91eux\x0b\x00\x01\x04\xe8\x03\x00\x00\x04d\x00\x00\x00'

    result = parse_extra_fields(extra_field)

    correct_result = {21589: {'mod_time': datetime.datetime(2023, 12, 31, 10, 51, 2)}, 30837: {'version': 1, 'uid': 1000, 'gid': 3}}

    assert result == correct_result

@pytest.mark.skipif(not os.path.isfile(f'{os.getenv("HOME")}/.s3cfg_scaleway'), reason="No scaleway credientials defined")
def test_s3_create_transport_client():
    '''Test if creation of transport credientials works'''
    config = configparser.ConfigParser()
    with open(f'{os.getenv("HOME")}/.s3cfg_scaleway') as f:
        config.read_file(f)
    client = create_transport_client(config)

    # Not much we can test here
    assert client._endpoint._endpoint_prefix == 's3'


# Local and s3 based zip file tests -----------------------------------------


@pytest.mark.parametrize('file_name,transport_client', testdata)
def test_find_central_dir(file_name, transport_client):
    with smart_open.open(file_name, 'rb', transport_params=dict(client=transport_client)) as f:
        assert find_central_dir(f) == 67


@pytest.mark.parametrize('file_name,transport_client', testdata)
def test_read_central_dir(file_name, transport_client):
    with smart_open.open(file_name, 'rb', transport_params=dict(client=transport_client)) as f:
        files_in_zip = read_central_dir(f)
    assert files_in_zip == {'empty.txt': {'date_time': datetime.datetime(2023, 12, 31, 12, 51, 2), 'length': 0, 'position': 0}}


@pytest.mark.parametrize('file_name,transport_client', testdata)
def test_pretty_print_files(file_name, transport_client):
    with smart_open.open(file_name, 'rb', transport_params=dict(client=transport_client)) as f:
        files_in_zip = read_central_dir(f)

    # The aim is to resemble normal zip command output as closely as possible
    pretty_print_target = '''Archive:  test_data/test1.zip
  Length      Date    Time    Name
---------  ---------- -----   ----
        0  2023-12-31 12:51   empty.txt
---------                     ----
        0                     1 files'''

    assert '\n'.join(pretty_print_files('test_data/test1.zip', files_in_zip)) == pretty_print_target


@pytest.mark.parametrize('file_name,transport_client', testdata)
def test_unzip_file_at_pos(file_name, transport_client):
    os.chdir('test_data') # Move to test_data directory so we don't mess up the main directory

    if file_name == 'test_data/test1.zip': file_name = 'test1.zip' # Hack to fix file url if we are testing against local file
    unzip_file_at_pos(file_name, 'empty.txt', 0, transport_client)

    assert os.path.isfile('empty.txt')

    os.remove('empty.txt') 
    os.chdir('..')


@pytest.mark.parametrize('file_name,transport_client', test2data)
def test_unzip_file_at_pos_streamout(file_name, transport_client):
    '''Open 10 byte file'''
    tome = io.BytesIO()
    unzip_file_at_pos(file_name, tome, 0, transport_client)

    assert tome.getvalue() == b'D \x82<\xfd\xe6\xf1\xc2k0'
    tome.close()


@pytest.mark.parametrize('file_name,transport_client', test3data)
def test_unzip_file_at_pos_streamout(file_name, transport_client):
    '''Open 10 000 byte file'''
    tome = io.BytesIO()
    unzip_file_at_pos(file_name, tome, 0, transport_client)

    assert hashlib.md5( tome.getvalue() ).hexdigest() == '3ed2e1443a3ab185f972a09ed6147789'
    tome.close()

