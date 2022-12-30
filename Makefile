MAKEFLAGS=-s

all: dist

dist: s3unzip/s3unzip.py
	python3 setup.py bdist_wheel
	cp -av dist/s3unzip-0.0.0-py3-none-any.whl .

clean: 
	@rm -rf build dist *.egg-info

