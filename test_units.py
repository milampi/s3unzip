#!/usr/bin/python3 -B
# -*- coding: utf-8 -*-

import pytest
#import imp
#from pytest import approx

from s3unzip import *

def test_find_central_dir():
    with open('test_data//test1.zip','rb') as f:
       assert find_central_dir(f) == 67

