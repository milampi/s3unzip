#!/usr/bin/python3 -B
# -*- coding: utf-8 -*-
# Copyright © 2022 Mikko Lampi <milampi@github.com>
# This file is licensed under GNU Lesser General Public License version 3 or later.

"""Commandline program and helper functions to read one file from a zip
that is inside S3 like object storage.

Zip format implementation gathered from:
    - Easy to read https://docs.fileformat.com/compression/zip/
    - Zip64 definition https://blog.yaakov.online/zip64-go-big-or-go-home/
    - Main url https://infozip.sourceforge.net/Info-ZIP.html
    - Github version of sourceforge for implementation details https://github.com/cysin/info-zip
    - Original zip definition https://pkware.cachefly.net/webdocs/casestudies/APPNOTE.TXT
    - More extra fields (Unix support) https://fossies.org/linux/zip/proginfo/extrafld.txt
"""

import io
import os
import sys
import argparse
import configparser
import struct
import fnmatch
from typing import Any, Iterable
import datetime
import errno

import boto3
import botocore # Needed for type checking
import stream_unzip
import smart_open
import chardet

def _extra_field_universal_time(hlen: int, hbytes: bytes, in_cd: bool) -> dict:
    """Parses extra time fields for Unix systems

    zip extra field id 0x5455:
    Unix Universal time https://fossies.org/linux/zip/proginfo/extrafld.txt

    This internal function will change a lot as all the miss implementations and strange behaviors
    from real life zip files will be found. Do not rely on this function.

    Parameters
    ----------
    hlen : bytes
        bytes from extra field in either local file or central directory
    hbytes : bytes
        bytes from extra field in either local file or central directory
    in_cd : bool
        is this record inside a local file (False) or central directory (True)

    Returns
    -------
    dict
        dict of information from fields in one header: { mod_time: xxxx, acc_time: xxxx, cre_time: xxxx }
    """

    fields = {}

    # Is this Central Directory header or Local File header
    if in_cd is True:
        # It is a CD header. It is processed differently from local file header
        # Is there a modtime
        if hlen > 1:
            secs = int.from_bytes(hbytes[1:5], byteorder='little')
        else:
            secs = 0
        fields['mod_time'] = datetime.datetime.utcfromtimestamp(secs)
    else:
        # It is a local file header
        flags = hbytes[0]
        hbytes = hbytes[1:] # Remove the fist byte that is flags

        if flags & 0b0000_0001:
            secs = int.from_bytes(hbytes[0:4], byteorder='little')
            fields['mod_time'] = datetime.datetime.utcfromtimestamp(secs)
            if len(hbytes) > 3:
                hbytes = hbytes[4:]
        if flags & 0b0000_0010:
            secs = int.from_bytes(hbytes[0:4], byteorder='little')
            fields['acc_time'] = datetime.datetime.utcfromtimestamp(secs)
            if len(hbytes) > 3:
                hbytes = hbytes[4:]
        if flags & 0b0000_0100:
            secs = int.from_bytes(hbytes[0:4], byteorder='little')
            fields['cre_time'] = datetime.datetime.utcfromtimestamp(secs)

    return fields


def _extra_field_uid_gid(hbytes: bytes) -> dict:
    """Parses extra field for uid and gid for Unix systems

    zip extra field id 0x7875:
    Unix UID/GID https://fossies.org/linux/zip/proginfo/extrafld.txt

    This internal function will change a lot as all the miss implementations and strange behaviors
    from real life zip files will be found. Do not rely on this function.

    Parameters
    ----------
    hbytes : bytes
        bytes from extra field in either local file or central directory

    Returns
    -------
    dict
        dict of information from fields in one header: { version: 1, gid: 1000, uid: 100 }
    """

    fields = {}

    # Only known version is 1. Everything else is considered an error and safely bypassed
    fields['version'] = hbytes[0]
    if hbytes[0] != 1:
        print(f'Unknown extra field Unix uid/gid version: {hbytes[0]}')
        return fields

    # Remove the fist byte that is flags
    hbytes = hbytes[1:]

    # uid
    uid_len = hbytes[0]
    hbytes = hbytes[1:] # Remove the uid_len byte
    fields['uid'] = int.from_bytes(hbytes[0:uid_len], byteorder='little')
    hbytes = hbytes[0:uid_len] # Remove the uid short/long

    # gid
    gid_len = hbytes[0]
    hbytes = hbytes[1:] # Remove the gid_len byte
    fields['gid'] = int.from_bytes(hbytes[0:gid_len], byteorder='little')

    return fields


def _extra_field_zip64_extended_info(hlen: int, hbytes: bytes) -> dict:
    """Parses extra field for zip64 extended info

    zip extra field id 0x0001:
    Zip64 extra fields https://fossies.org/linux/zip/proginfo/extrafld.txt

    This internal function will change a lot as all the miss implementations and strange behaviors
    from real life zip files will be found. Do not rely on this function.

    Parameters
    ----------
    hbytes : bytes
        bytes from extra field in either local file or central directory

    Returns
    -------
    dict
        dict of information from fields in one header: { 'uncomp_len': 4311744512, 'comp_len': 4312442995 ... }
    """

    parse_map = '<Q'
    field_names = 'uncomp_len'

    if hlen>8:
       parse_map +='Q'
       field_names += ' comp_len'

    if hlen>16:
       parse_map +='Q'
       field_names += ' start'

    if hlen>24:
       parse_map +='I'
       field_names += ' disk_num'

    # If all fields get replaces. It will look like:
    # parse_map = '<QQQI'
    # field_names = 'uncomp_len comp_len start disk_num'

    fields = struct.unpack(parse_map, hbytes)
    zip64extra = dict(zip(field_names.split(), fields))

    return zip64extra


def parse_extra_fields(extra_bytes: bytes, overwrite: dict, in_cd: bool = True) -> dict:
    """Parses extra fields in a record

    Parses the extra fields.
    https://pkware.cachefly.net/webdocs/casestudies/APPNOTE.TXT
    4.5 Extensible data fields

    Parameters
    ----------
    extra_bytes : bytes
        bytes from extra field in either local file or central directory
    in_cd : bool
        is this record inside a local file (False) or central directory (True)

    Returns
    -------
    dict
        Dict of dicts. Made from extra field header id values that contain a dict of information from those fields
    """

    extra_fields: dict = {}
    bstream = io.BytesIO(extra_bytes)

    while True:
        head = bstream.read(4)

        # Check for eof
        if not head:
            break

        (hid, hlen) = struct.unpack('<HH', head)
        hbytes = bstream.read(hlen)

        # Definitions from https://fossies.org/linux/zip/proginfo/extrafld.txt

        # Universal time
        if   hid == 0x5455:
            extra_fields[hid] = _extra_field_universal_time(hlen, hbytes, in_cd)
            # Adds: 0x5455: { mod_time: xxxx, acc_time: xxxx, cre_time: xxxx }
        # Unix UID/GID
        elif hid == 0x7875:
            extra_fields[hid] = _extra_field_uid_gid(hbytes)
            # Adds: 0x7875: { version: 1, gid: 1000, uid: 100 }
        # Zip64 extended info
        elif hid == 0x0001:
            extra_fields[hid] = _extra_field_zip64_extended_info(hlen, hbytes)
            overwrite.update(extra_fields[hid])
            # Adds: 0x0001: { 'uncomp_len': 4311744512, 'comp_len': 4312442995 ... }
        else:
            print(f'Unknown field hid:0x{hid:02x} len:{hlen} bytes:{hbytes} (parse_extra_fields)')
            extra_fields[hid] = hbytes

    return extra_fields


def parse_local_file_header(s3_stream: io.BufferedIOBase) -> dict:
    """Parses local file header

    This record is embedded in zip stream, and not in the central file directory.
    Currently this function is never needed, as the central directory is the final authority.
    https://docs.fileformat.com/compression/zip/#local-file-header

    Parameters
    ----------
    s3_stream : io.BufferedIOBase
        filestream where the file header line is read from

    Returns
    -------
    dict
        Dict of fields from the header record: (minver flags method
        time date crc32 comp_len uncomp_len fname_len extra_len)
    """

    # Parse the binary record of zip header and bind values to names in a dict
    fields = struct.unpack('<HHHHHIIIHH', s3_stream.read(26))
    lfh = dict(zip('minver flags method time date crc32 comp_len uncomp_len fname_len extra_len'.split(), fields))

    # Read but bypass the filename. It is redundant. Central directory is the final truth about filenames.
    # Guess the encoding of the filename string
    fname_bytes = s3_stream.read(lfh['fname_len'])
    fname_encoding = chardet.detect(fname_bytes)['encoding']
    lfh['fname'] = fname_bytes.decode(fname_encoding)

    # Parse the extra fields
    extra_bytes = s3_stream.read(lfh['extra_len'])
    lfh['extra'] = parse_extra_fields(extra_bytes, lfh, False)

    # Check if the compressed file is a zip64 file. Meaning larger than 2 GB TODO add zip64 support
    if lfh['comp_len'] == 0xffffffff or lfh['uncomp_len'] == 0xffffffff:
        print('zip64 not supported yet (parse_local_file_header)')

    return lfh


def parse_central_dir_file_header(s3_stream: io.BufferedIOBase) -> dict:
    """Parses central directory file header

    This record is embedded in the central file directory.
    https://docs.fileformat.com/compression/zip/#central-directory-file-header

    Parameters
    ----------
    s3_stream : io.BufferedIOBase
        filestream where the central directory file header is read from

    Returns
    -------
    dict
        Dict of fields from the header record: (ver minver flags method
        time date crc32 comp_len uncomp_len fname_len extra_len extra
        comment_len comment disk_num iattr eattr start)

    """

    # Parse the record of zip central directory file header and bind values to names in a dict
    fields = struct.unpack('<HHHHHHIIIHHHHHII', s3_stream.read(42))
    cdfh = dict(zip('ver minver flags method time date crc32 comp_len uncomp_len fname_len '
                    'extra_len comment_len disk_num iattr eattr start'.split(), fields))

    # Guess the encoding of the filename string
    fname_bytes = s3_stream.read(cdfh['fname_len'])
    fname_encoding = chardet.detect(fname_bytes)['encoding']
    cdfh['fname'] = fname_bytes.decode(fname_encoding)

    # Parse date and time fields. These are the old DOS fields. Formula inspired by zipfile library
    cdfh['date_time'] = datetime.datetime((cdfh['date']>>9)+1980, (cdfh['date']>>5)&0xF, cdfh['date']&0x1F,
                                          cdfh['time']>>11, (cdfh['time']>>5)&0x3F, (cdfh['time']&0x1F) * 2)

    # Parse the extra fields
    extra_bytes = s3_stream.read(cdfh['extra_len'])
    cdfh['extra'] = parse_extra_fields(extra_bytes, cdfh, True)

    # Guess the encoding of the comment string
    if cdfh['comment_len'] > 0:
        comment_bytes = s3_stream.read(cdfh['comment_len'])
        comment_encoding = chardet.detect(comment_bytes)['encoding']
        cdfh['comment'] = comment_bytes.decode(comment_encoding)
    else:
        cdfh['comment'] = ''

    return cdfh


def parse_endof_central_dir_record(s3_stream: io.BufferedIOBase) -> dict:
    """Parses end of central directory record

    This record marks the end of the central file directory.
    It is mainly used to find the beginning of the central directory.
    https://docs.fileformat.com/compression/zip/#end-of-central-directory-record

    Parameters
    ----------
    s3_stream : io.BufferedIOBase
        filestream where the end of central directory record is read from

    Returns
    -------
    dict
        Dict of fields from the header record: (disk_num dir_disk_num
        disk_records_num tot_records_num dir_len dir_start comment_len comment)
    """

    fields = struct.unpack('<HHHHIIH', s3_stream.read(18))
    ecdr = dict(zip('disk_num dir_disk_num disk_records_num tot_records_num dir_len dir_start comment_len'.split(), fields))

    # Guess the encoding of the comment string
    if ecdr['comment_len'] > 0:
        comment_bytes = s3_stream.read(ecdr['comment_len'])
        comment_encoding = chardet.detect(comment_bytes)['encoding']
        ecdr['comment'] = comment_bytes.decode(comment_encoding)
    else:
        ecdr['comment'] = ''

    return ecdr


def parse_endof_central_dir_record_zip64(s3_stream: io.BufferedIOBase) -> dict:
    """Parses end of central directory record zip64 version

    This record marks the end of the central file directory.
    It is mainly used to find the beginning of the central directory.
    https://docs.fileformat.com/compression/zip/#end-of-central-directory-record

    Parameters
    ----------
    s3_stream : io.BufferedIOBase
        filestream where the end of central directory record is read from

    Returns
    -------
    dict
        Dict of fields from the header record: ('len ver minver disk_num
        dir_disk_num disk_records_num tot_records_num dir_len dir_start comment)
    """

    fields = struct.unpack('<QHHIIQQQQ', s3_stream.read(52))
    ecdr64 = dict(zip('len ver minver disk_num dir_disk_num disk_records_num tot_records_num dir_len dir_start'.split(), fields))

    # Guess the encoding of the comment string
    if ecdr64['len'] > 52: # We have some comment characters
        comment_bytes = s3_stream.read(ecdr64['len']-52)
        comment_encoding = chardet.detect(comment_bytes)['encoding']
        ecdr64['comment'] = comment_bytes.decode(comment_encoding)
    else:
        ecdr64['comment'] = ''

    return ecdr64


def parse_endof_central_dir_locator_zip64(s3_stream: io.BufferedIOBase) -> dict:
    """Parses end of central directory locator zip64 version

    This record marks the end of the central file directory.
    It is mainly used to find the beginning of the central directory.
    https://docs.fileformat.com/compression/zip/#end-of-central-directory-record

    Parameters
    ----------
    s3_stream : io.BufferedIOBase
        filestream where the end of central directory locator is read from

    Returns
    -------
    dict
        Dict of fields from the header record: ('disk_num start_zip64 tot_disk_num')
    """

    fields = struct.unpack('<IQI', s3_stream.read(16))
    ecdl64 = dict(zip('disk_num start_zip64 tot_disk_num'.split(), fields))

    return ecdl64


def unzip_file_at_pos(s3_file_name: str, out_file_name: Any, pos: int, client: Any) -> None:
    """Unzip file at position

    Parameters
    ----------
    s3_file_name : str
        Url of file from which compressed data is read from
    out_file_name : Any
        Name of file into which the uncompressed data is written, or
        file descriptor into which data is written
    pos : int
        At which byte position in file the file is read from
    client :
        boto s3 client connection where the zip stream is read from
    """

    # Open the output file, or use file stream that has been provided
 
    if isinstance(out_file_name, str):
       out=open(out_file_name, 'wb')
    else:
       out=out_file_name # out_file_name is a stream like sys.stdout

    try:
        # Read chunk by chunk from s3 and write to output file
        with smart_open.open(s3_file_name, 'rb', transport_params=dict(client=client)) as s3_stream:
            # Seek in s3 to start position
            s3_stream.seek(pos, 0)
    
            # Read chunk by chunk
            # for file_local_name, file_size, unzipped_chunks in stream_unzip.stream_unzip(f):
            for _, _, unzipped_chunks in stream_unzip.stream_unzip(s3_stream):
                # unzipped_chunks must be iterated to completion or
                # UnfinishedIterationError will be raised
                for chunk in unzipped_chunks:
                    if out==sys.stdout:
                        sys.stdout.buffer.write(chunk)
                    else:
                        out.write(chunk)
                break

    finally:
        # Close the output file if we opened it
        if isinstance(out_file_name, str):
            out.close()

    return


def find_central_dir(s3_stream: io.BufferedIOBase) -> int:
    """Tries to find the beginning of the Central Directory of a zip file.
       Central Directory is at the end of the zip file.
       If the zip file has been appended, there might be multiple ones. Only the last one is valid.

    Parameters
    ----------
    s3_stream : io.BufferedIOBase
        filestream where the end of central directory record is read from

    Returns
    -------
    int
        File offset where the End of Central Directory Record starts
        Can be 32 or 64 bits long.
    """

    # Try to find "End of Central Directory" Record
    # Seek from the end of file the maximum length of comment 65 kB and End of Central Directory Record 18 B + header 4 B
    try:
        s3_stream.seek(-65536 - 18 - 4, io.SEEK_END) # Start searching -65558 bytes from the end of file.
    except OSError as err_in_seek:
        if err_in_seek.errno == errno.EINVAL: # "Invalid argument" means file was less than 65558 long
            s3_stream.seek(0, io.SEEK_SET) # The file is very small. Start searching from beginning of file
        else:
            raise

    # Read the maximum size of directory block
    byte_block = s3_stream.read(65536 + 18 + 4)

    # TODO Look for multiple markers and choose the last one
    # Find from the binary block the marker for End of Central Directory
    addr = byte_block.find(b'\x50\x4b\x05\x06')
    if addr == -1:
        Exception('No End of Central Directory Record found')

    # Parse se buffer as a file stream
    ecdr = parse_endof_central_dir_record( io.BytesIO(byte_block[addr+4:addr+4+18]) )
 
    # TODO Sanity check. Check if it is just a random number sequence (Is there a CD at the end of the pointed field)

    cd_pos = ecdr['dir_start']

    # zip64 version of the record has cd_pos as 0xffffffff. Find and parse se zip64 record instead
    if cd_pos == 0xffffffff:
        # Find from the binary block the marker for Central Directory
        addr = byte_block.find(b'\x50\x4b\x06\x06')
        if addr == -1:
            Exception('No End of Central Directory Record (zip64) found')

        len_field = struct.unpack('<Q', byte_block[addr+4:addr+4+8] )
        ecdr64 = parse_endof_central_dir_record_zip64( io.BytesIO(byte_block[addr+4:addr+4+len_field[0]+8]) )
        cd_pos = ecdr64['dir_start']

    # TODO Sanity check. Check if this CDR is the actually last one, or if there is one more after this.
    # TODO Check if the information about multiple CDR when zip has been appended is correct

    return cd_pos


def read_central_dir(s3_stream: io.BufferedIOBase) -> dict:
    """Reads and parses the central directory of a zip file

    Parameters
    ----------
    s3_stream : io.BufferedIOBase
        Filestream where the central directory record is read from.
        Must be positioned at the beginning of the central dir record.

    Returns
    -------
    dict
        Dict of filenames with their start offsets in zip file
        Example:
        { 'empty.txt': {'position': 0, 'length': 0, 'date_time': datetime.datetime(2023, 12, 31, 12, 51, 2)} }
    """

    # Read through central directory and create a list of files in it
    files_in_zip: dict = {}
    while True:
        byte_block = s3_stream.read(4)
        if len(byte_block) < 4:
            break # EOF

        (header_id,) = struct.unpack('<L', byte_block)

        #print(f'Header ID: 0x{header_id:08x}')

        # Central Directory File Header
        if header_id == 0x02014b50:
            cdfh = parse_central_dir_file_header(s3_stream)
            files_in_zip[cdfh['fname']] = {}
            files_in_zip[cdfh['fname']]['position'] = cdfh['start']
            files_in_zip[cdfh['fname']]['length'] = cdfh['uncomp_len']
            files_in_zip[cdfh['fname']]['date_time'] = cdfh['date_time']
            files_in_zip[cdfh['fname']]['extra'] = cdfh['extra']
        # End of Central Directory Record
        elif header_id == 0x06054b50:
            parse_endof_central_dir_record(s3_stream)
        # Zip64 end of central directory record
        elif header_id == 0x06064b50:
            parse_endof_central_dir_record_zip64(s3_stream)
        # Zip64 end of central directory locator
        elif header_id == 0x07064b50:
            # https://pkware.cachefly.net/webdocs/casestudies/APPNOTE.TXT
            parse_endof_central_dir_locator_zip64(s3_stream)
        else:
            print(f'Unknown header: 0x{header_id:08x} (read_central_dir)')
            break

    return files_in_zip


def create_transport_client(config: configparser.ConfigParser) -> botocore.client.BaseClient:
    """Return a transportation client object to define how to connect to the .zip file

    Parameters
    ----------
    config : configparser.ConfigParser
        S3 credentials and configuration

    Returns
    -------
    botocore.client.BaseClient
        Transportation client information for .zip file
    """

    # Checks whether to use http or https connection
    if config['default']['use_https'] is True:
        url = f'https://{config["default"]["host_base"]}'
    else:
        url = f'http://{config["default"]["host_base"]}'

    # Build a session that connects to the right s3 bucket
    session = boto3.Session(region_name=config['default']['bucket_location'],
                            aws_access_key_id=config['default']['access_key'],
                            aws_secret_access_key=config['default']['secret_key'])
    client = session.client('s3', endpoint_url=url)

    return client


def pretty_print_files(archive_name: str, files_in_zip: dict) -> Iterable[str]:
    """A generator that pretty prints the central directory of a zip file line by line

    Parameters
    ----------
    archive_name : str
        Name of the zip file
    files_in_zip : dict
        List of files

    Returns
    -------
    Iterable[str]
        Generator giving lines of output for all files in zip.
        Also header and other pretty print related non file text.
    """

    # Sum of all file sizes. The uncompressed size of zip file contents
    file_size_sum = 0

    yield f'Archive:  {archive_name}'
    yield '  Length      Date    Time    Name'
    yield '---------  ---------- -----   ----'
    for file_name, fields in files_in_zip.items():
        file_size_sum += fields["length"]
        yield f'{fields["length"]:9d}  {fields["date_time"].isoformat(sep=" ", timespec="minutes")}   {file_name}'
    yield '---------                     ----'
    yield f'{file_size_sum:9d}                     {len(files_in_zip)} files'


def main() -> None:
    """Main routine to run from commandline

    """

    # TODO old pylint version flags variables as constants here. Remove the disable after pylint upgrade
    # pylint: disable=C0103
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='s3unzip unzips files directly from a s3 object.')
    parser.add_argument('zipfile', help='S3 based zip archive to read from. Example: s3://bucket/object.zip')
    parser.add_argument('file', nargs='*', help='Files to unzip from archive. Example: "somefile*.json"')
    parser.add_argument('-l', '--list', action='store_true', help='List files in zip.')
    parser.add_argument('-p', '--pipe', action='store_true', help='Pipe file to stdout.')
    parser.add_argument('-e', '--env', default=f'{os.getenv("HOME")}/.s3cfg', help='S3 access config. Default: %(default)s')
    # --toc-show-addr --toc-search-start --toc-from-file # TODO table of contents control
    # -x -d -q -qq
    # -t -z -j
    # -n -o
    # -P
    args = parser.parse_args()

    # If we are reading a s3 based file. Create a transportation client for it.
    # If we are not, just connect directly (Set client to None)
    client = None
    if args.zipfile.startswith('s3://'):
        # Reads in config used by s3cmd and uses the login credentials from it.
        # s3cmd config is usually in file ~/.s3cfg
        config = configparser.ConfigParser()
        try:
            with open(args.env) as f:
                config.read_file(f)
        except FileNotFoundError as e:
            print(e)
            exit(-1)

        # Create a transportation client object to define how to connect to the .zip file
        client = create_transport_client(config)

    # Read the central directory of the s3 file
    try:
        with smart_open.open(args.zipfile, 'rb', transport_params=dict(client=client)) as f:
            # Find and seek to beginning of central directory
            cd_pos = find_central_dir(f)
            f.seek(cd_pos, 0)

            # Read the full central directory
            files_in_zip = read_central_dir(f)
    except (FileNotFoundError, OSError) as e:
        print(e)
        exit(-1)

    # Pretty print file information in zip and quit
    if args.list is True:
        for line in pretty_print_files(args.zipfile, files_in_zip):
            print(line)
        exit(0)

    # Get files that match
    for file_name, fields in files_in_zip.items():
        position = fields['position']
        for file_regexp in args.file:
            if fnmatch.fnmatchcase(file_name, file_regexp):
                if args.pipe is False:
                    # Unzip into a file
                    print('  inflating:', file_name)

                    path = os.path.dirname(file_name)
                    # TODO parse & prevent paths to only go downwards from where we are now
                    if len(path) > 0:
                        os.makedirs(path, exist_ok=True) # Create path for file if needed

                    unzip_file_at_pos(args.zipfile, file_name, position, client)

                else:
                    # Unzip to stdout
                    unzip_file_at_pos(args.zipfile, sys.stdout, position, client)

# Operations done only if we are run on the commandline
if __name__ == '__main__':
    main()

