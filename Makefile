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

# Generate test data for unit tests
gentestdata: test_data/test2.zip test_data/test3.zip test_data/test4.zip

# this dataset is needed only when testing 64 bit support. Usually not worth the disk space. Generated separately
gentestdatabig: test_data/test_big.zip

# random.seed(1) is used for the test data, to give us repeatable random like data
test_data/test2.zip:
	python3 -c 'import random,sys; random.seed(1); sys.stdout.buffer.write(bytes([ random.randint(0,255) for _ in range(10) ]))' >test_data/small.bin
	(cd test_data; zip test2.zip small.bin)
	@rm -f test_data/small.bin

test_data/test3.zip:
	python3 -c 'import random,sys; random.seed(1); sys.stdout.buffer.write(bytes([ random.randint(0,255) for _ in range(10000) ]))' >test_data/small.bin
	(cd test_data; zip test3.zip small.bin)
	@rm -f test_data/small.bin

test_data/test4.zip:
	python3 -c 'import random,sys; random.seed(1); sys.stdout.buffer.write(bytes([ random.randint(0,255) for _ in range(20001) ]))' >test_data/small1.bin
	python3 -c 'import random,sys; random.seed(1); sys.stdout.buffer.write(bytes([ random.randint(0,255) for _ in range(20002) ]))' >test_data/small2.bin
	python3 -c 'import random,sys; random.seed(1); sys.stdout.buffer.write(bytes([ random.randint(0,255) for _ in range(20003) ]))' >test_data/small3.bin
	python3 -c 'import random,sys; random.seed(1); sys.stdout.buffer.write(bytes([ random.randint(0,255) for _ in range(20004) ]))' >test_data/small4.bin
	(cd test_data; zip test4.zip small1.bin small2.bin small3.bin small4.bin)
	@rm -f test_data/small?.bin

test_data/test_big.zip:
	python3 -c 'import random,sys; random.seed(1); b=bytes([ random.randint(0,255) for _ in range(pow(2,24)) ]); [ sys.stdout.buffer.write(b) for _ in range(257) ]' >test_data/big.bin
	(cd test_data; zip test_big.zip big.bin)
	@rm -f test_data/big.bin

