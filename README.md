# How to unzip a single file from a zip inside S3

`s3unzip` is a small command line program to unzip a file from a zip that is inside a S3 bucket. It tries to resemble both in output and command line arguments the real unzip program, but that is just for convenience. Most of the original unzip flags would make no sense in S3 context.

# Usage example

```
s3unzip s3://my_bucket/very_big.zip path_inside/file_wanted.json
```

# Configuration

Access tokens and other configuration is read from `~/.s3cfg` . If you have a working s3cmd you probably have everything set up already.

# Current state of software

This is "works on my computer" level pre alpha 0.0.0 type of software. Not for production use. The easy part of understanding zip format and S3 and libraries relevant to the task is over. Software has been written and does what it is expected to do. Documentation is under works and should be rather small.
The big and time consuming part of building the testing environment is the next part. The program has been currently tested under Linux and against Scaleway S3 service. Building a testing harness will start with Minio, AWS, GC, Azure and after that with targets that seem most relevant.
s3unzip is based on boto3 and smart\_open for S3 connection. Things they are compatible with should work. streamed unzipping is done with stream\_unzip. One of the main drivers of the project has been not to do the fun thing and make everything myself, but compare and find libraries that are a good fit for the problem.

# Installation instructions

```
pip3 install s3unzip-0.0.0-py3-none-any.whl
```

# TODO
- Integration tests for different S3 services
- Better heuristics for finding the last directory entry

