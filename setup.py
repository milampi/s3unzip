import subprocess
from setuptools import find_packages, setup

setup(
    name     = 's3unzip',
    packages = find_packages(include=['s3unzip']),
    #version  = gitversion,
    install_requires = [ 'boto3', 'stream_unzip', 'smart_open', 'chardet' ],
    description = 's3unzip unzips files in zips inside s3 to local directory without downloading the zip file fully',
    author  = 'milampi@github.com',
    license = 'GPL-3',
)

