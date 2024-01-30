MAKEFLAGS=-s

all: dist

dist: s3unzip/s3unzip.py
	python3 setup.py bdist_wheel
	cp -av dist/s3unzip-0.0.0-py3-none-any.whl .

clean: 
	@rm -rf build dist *.egg-info

test: export PYTHONDONTWRITEBYTECODE = 1
test:
	py.test3 -p no:cacheprovider -v test_units.py

gentestdata:
	python3 -c 'import random,sys; random.seed(1); sys.stdout.buffer.write(bytes([ random.randint(0,255) for _ in range(10) ]))' >test_data/small.bin
	(cd test_data; zip test2.zip small.bin)
	@rm -f test_data/small.bin

