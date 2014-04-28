#!/usr/bin/python -u
# Copyright (c) 2010-2012 OpenStack, LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import print_function

import signal
import socket
import logging

from optparse import OptionParser, SUPPRESS_HELP
from os import environ, walk, _exit as os_exit
from os.path import isfile, isdir, join
from sys import argv as sys_argv, exit, stderr
from time import gmtime, strftime

from swiftclient import RequestException
from swiftclient.utils import config_true_value, prt_bytes
from swiftclient.multithreading import OutputManager
from swiftclient.exceptions import ClientException
from swiftclient import __version__ as client_version
from swiftclient.service import SwiftService, SwiftError
from swiftclient.command_helpers import print_account_headers, \
    print_container_headers, print_object_headers


BASENAME = 'swift'


def immediate_exit(signum, frame):
    stderr.write(" Aborted\n")
    os_exit(2)

st_delete_options = '''[-all] [--leave-segments]
                    [--object-threads <threads>]
                    [--container-threads <threads>]
                    <container> [object]
'''

st_delete_help = '''
Deletes a container or objects within a container

Positional arguments:
  <container>           Name of container to delete from.
  [object]              Name of object to delete. Specify multiple times
                        for multiple objects.

Optional arguments:
  --all                 Delete all containers and objects.
  --leave-segments      Do not delete segments of manifest objects.
  --object-threads <threads>
                        Number of threads to use for deleting objects.
                        Default is 10.
  --container-threads <threads>
                        Number of threads to use for deleting containers.
                        Default is 10.
'''.strip("\n")


def st_delete(parser, args, output_manager):
    parser.add_option(
        '-a', '--all', action='store_true', dest='yes_all',
        default=False, help='Delete all containers and objects.')
    parser.add_option(
        '', '--leave-segments', action='store_true',
        dest='leave_segments', default=False,
        help='Do not delete segments of manifest objects.')
    parser.add_option(
        '', '--object-threads', type=int,
        default=10, help='Number of threads to use for deleting objects. '
        'Default is 10.')
    parser.add_option('', '--container-threads', type=int,
                      default=10, help='Number of threads to use for '
                      'deleting containers. '
                      'Default is 10.')
    (options, args) = parse_args(parser, args)
    args = args[1:]
    if (not args and not options.yes_all) or (args and options.yes_all):
        output_manager.error('Usage: %s delete %s\n%s',
                             BASENAME, st_delete_options,
                             st_delete_help)
        return

    _opts = vars(options)
    _opts['object_dd_threads'] = options.object_threads
    with SwiftService(options=_opts) as swift:
        try:
            if not args:
                _del_iter = swift.delete()
            else:
                container = args[0]
                if '/' in container:
                    output_manager.error(
                        'WARNING: / in container name; you '
                        'might have meant %r instead of %r.' % (
                        container.replace('/', ' ', 1), container)
                    )
                    return
                objects = args[1:]
                if objects:
                    _del_iter = swift.delete(container=container,
                                             objects=objects)
                else:
                    _del_iter = swift.delete(container=container)

            for _r in _del_iter:
                if _r['success']:
                    if options.verbose > 1:
                        if _r['action'] == 'delete_object':
                            _c = _r['container']
                            _o = _r['object']
                            _p = '%s/%s' % (_c, _o) if options.yes_all else _o
                            _a = _r['attempts']
                            if _a > 1:
                                output_manager.print_msg(
                                    '%s [after %d attempts]', _p, _a)
                            else:
                                output_manager.print_msg(_p)

                        elif _r['action'] == 'delete_segment':
                            _c = _r['container']
                            _o = _r['object']
                            _p = '%s/%s' % (_c, _o)
                            _a = _r['attempts']
                            if _a > 1:
                                output_manager.print_msg(
                                    '%s [after %d attempts]', _p, _a)
                            else:
                                output_manager.print_msg(_p)

                else:
                    # Special case error prints
                    output_manager.error("An unexpected error occurred whilst"
                                         "deleting: %s" % _r['error'])
        except SwiftError as err:
            output_manager.error(err.value)


st_download_options = '''[--all] [--marker] [--prefix <prefix>]
                      [--output <out_file>] [--object-threads <threads>]
                      [--container-threads <threads>] [--no-download]
                      [--skip-identical] <container> <object>
'''

st_download_help = '''
Download objects from containers

Positional arguments:
  <container>           Name of container to download from. To download a
                        whole account, omit this and specify --all.
  <object>              Name of object to download. Specify multiple times
                        for multiple objects. Omit this to download all
                        objects from the container.

Optional arguments:
  --all                 Indicates that you really want to download
                        everything in the account.
  --marker              Marker to use when starting a container or account
                        download.
  --prefix <prefix>     Only download items beginning with <prefix>
  --output <out_file>   For a single file download, stream the output to
                        <out_file>. Specifying "-" as <out_file> will
                        redirect to stdout.
  --object-threads <threads>
                        Number of threads to use for downloading objects.
                        Default is 10.
  --container-threads <threads>
                        Number of threads to use for downloading containers.
                        Default is 10.
  --no-download         Perform download(s), but don't actually write anything
                        to disk.
  --header <header_name:header_value>
                        Adds a customized request header to the query, like
                        "Range" or "If-Match". This argument is repeatable.
                        Example --header "content-type:text/plain"
  --skip-identical      Skip downloading files that are identical on both
                        sides.
  --no-clobber          Skip downloading files that already exist.
'''.strip("\n")


def st_download(parser, args, output_manager):
    parser.add_option(
        '-a', '--all', action='store_true', dest='yes_all',
        default=False, help='Indicates that you really want to download '
        'everything in the account.')
    parser.add_option(
        '-m', '--marker', dest='marker',
        default='', help='Marker to use when starting a container or '
        'account download.')
    parser.add_option(
        '-p', '--prefix', dest='prefix',
        help='Only download items beginning with the <prefix>.')
    parser.add_option(
        '-o', '--output', dest='out_file', help='For a single '
        'download, stream the output to <out_file>. '
        'Specifying "-" as <out_file> will redirect to stdout.')
    parser.add_option(
        '', '--object-threads', type=int,
        default=10, help='Number of threads to use for downloading objects. '
        'Default is 10.')
    parser.add_option(
        '', '--container-threads', type=int, default=10,
        help='Number of threads to use for downloading containers. '
        'Default is 10.')
    parser.add_option(
        '', '--no-download', action='store_true',
        default=False,
        help="Perform download(s), but don't actually write anything to disk.")
    parser.add_option(
        '-H', '--header', action='append', dest='header',
        default=[],
        help='Adds a customized request header to the query, like "Range" or '
        '"If-Match". This argument is repeatable. '
        'Example: --header "content-type:text/plain"')
    parser.add_option(
        '--skip-identical', action='store_true', dest='skip_identical',
        default=False, help='Skip downloading files that are identical on '
        'both sides.')
    parser.add_option(
        '--no-clobber', action='store_true', dest='no_clobber', default=False,
        help='Skip downloading files that already exist.')
    (options, args) = parse_args(parser, args)
    args = args[1:]
    if options.out_file == '-':
        options.verbose = 0

    if options.out_file and len(args) != 2:
        exit('-o option only allowed for single file downloads')

    if (not args and not options.yes_all) or (args and options.yes_all):
        output_manager.error('Usage: %s download %s\n%s', BASENAME,
                             st_download_options, st_download_help)
        return

    _opts = vars(options)
    _opts['object_dd_threads'] = options.object_threads
    with SwiftService(options=_opts) as swift:
        try:
            if not args:
                _down_iter = swift.download()
            else:
                container = args[0]
                if '/' in container:
                    output_manager.error(
                        'WARNING: / in container name; you '
                        'might have meant %r instead of %r.' % (
                        container.replace('/', ' ', 1), container)
                    )
                    return
                objects = args[1:]
                if not objects:
                    _down_iter = swift.download(container)
                else:
                    _down_iter = swift.download(container, objects)

            for _down in _down_iter:
                if options.out_file == '-' and 'contents' in _down:
                    for chunk in _down['contents']:
                        output_manager.print_msg(chunk)
                else:
                    if _down['success']:
                        if options.verbose:
                            start_time = _down['start_time']
                            headers_receipt = \
                                _down['headers_receipt'] - start_time
                            auth_time = _down['auth_end_time'] - start_time
                            finish_time = _down['finish_time']
                            read_length = _down['read_length']
                            attempts = _down['attempts']
                            total_time = finish_time - start_time
                            download_time = total_time - auth_time
                            _mega = 1000000
                            speed = float(read_length) / download_time / _mega
                            time_str = (
                                'auth %.3fs, headers %.3fs, total %.3fs, '
                                '%.3f MB/s' % (
                                    auth_time, headers_receipt,
                                    total_time, speed
                                )
                            )
                            path = _down['path']
                            if attempts > 1:
                                output_manager.print_msg(
                                    '%s [%s after %d attempts]',
                                    path, time_str, attempts
                                )
                            else:
                                output_manager.print_msg(
                                    '%s [%s]', path, time_str
                                )
                    else:
                        error = _down['error']
                        path = _down['path']
                        container = _down['container']
                        obj = _down['object']
                        if isinstance(error, ClientException):
                            if error.http_status == 304 and \
                                    options.skip_identical:
                                output_manager.print_msg(
                                    "Skipped identical file '%s'", path)
                                continue
                            if error.http_status == 404:
                                output_manager.error(
                                    "Object '%s/%s' not found", container, obj)
                                continue
                        output_manager.error(
                            "Error downloading object '%s/%s': %s",
                            container, obj, error)

        except SwiftError as e:
            output_manager.error(e.value)


st_list_options = '''[--long] [--lh] [--totals] [--prefix <prefix>]
                  [--delimiter <delimiter>]
'''
st_list_help = '''
Lists the containers for the account or the objects for a container

Positional arguments:
  [container]           Name of container to list object in.

Optional arguments:
  --long                Long listing format, similar to ls -l.
  --lh                  Report sizes in human readable format similar to
                        ls -lh.
  --totals              Used with -l or --lh, only report totals.
  --prefix              Only list items beginning with the prefix.
  --delimiter           Roll up items with the given delimiter. For containers
                        only. See OpenStack Swift API documentation for what
                        this means.
'''.strip('\n')


def st_list(parser, args, output_manager):
    parser.add_option(
        '-l', '--long', dest='long', action='store_true', default=False,
        help='Long listing format, similar to ls -l.')
    parser.add_option(
        '--lh', dest='human', action='store_true',
        default=False, help='Report sizes in human readable format, '
        "similar to ls -lh.")
    parser.add_option(
        '-t', '--totals', dest='totals',
        help='used with -l or --lh, only report totals.',
        action='store_true', default=False)
    parser.add_option(
        '-p', '--prefix', dest='prefix',
        help='Only list items beginning with the prefix.')
    parser.add_option(
        '-d', '--delimiter', dest='delimiter',
        help='Roll up items with the given delimiter. For containers '
             'only. See OpenStack Swift API documentation for '
             'what this means.')
    (options, args) = parse_args(parser, args)
    args = args[1:]
    if options.delimiter and not args:
        exit('-d option only allowed for container listings')

    _opts = vars(options).copy()
    if _opts['human']:
        _opts.pop('human')
        _opts['long'] = True

    with SwiftService(options=_opts) as swift:
        try:
            if not args:
                stats = swift.list()
            else:
                container = args[0]
                args = args[1:]
                if "/" in container or args:
                    output_manager.error(
                        'Usage: %s list %s\n%s', BASENAME,
                        st_list_options, st_list_help)
                    return
                else:
                    stats = swift.list(container=container)

            total_count = total_bytes = 0
            container = stats.get("container", None)
            for item in stats["listing"]:
                item_name = item.get('name')

                if not options.long and not options.human:
                    output_manager.print_msg(
                        item.get('name', item.get('subdir')))
                else:
                    item_bytes = item.get('bytes')
                    total_bytes += item_bytes
                    if not container:    # listing containers
                        byte_str = prt_bytes(item_bytes, options.human)
                        count = item.get('count')
                        total_count += count
                        try:
                            meta = item.get('meta')
                            utc = gmtime(float(meta.get('x-timestamp')))
                            datestamp = strftime('%Y-%m-%d %H:%M:%S', utc)
                        except ClientException:
                            datestamp = '????-??-?? ??:??:??'
                        if not options.totals:
                            output_manager.print_msg(
                                "%5s %s %s %s", count, byte_str,
                                datestamp, item_name)
                    else:    # list container contents
                        subdir = item.get('subdir')
                        if subdir is None:
                            byte_str = prt_bytes(
                                item_bytes, options.human)
                            date, xtime = item.get(
                                'last_modified').split('T')
                            xtime = xtime.split('.')[0]
                        else:
                            byte_str = prt_bytes(0, options.human)
                            date = xtime = ''
                            item_name = subdir
                        if not options.totals:
                            output_manager.print_msg(
                                "%s %10s %8s %s", byte_str, date,
                                xtime, item_name)

            # report totals
            if options.long or options.human:
                if not container:
                    output_manager.print_msg(
                        "%5s %s", prt_bytes(total_count, True),
                        prt_bytes(total_bytes, options.human))
                else:
                    output_manager.print_msg(
                        prt_bytes(total_bytes, options.human))

        except SwiftError as e:
            output_manager.error(e.value)


st_stat_options = '''[--lh]
                  [container] [object]
'''

st_stat_help = '''
Displays information for the account, container, or object

Positional arguments:
  [container]           Name of container to stat from.
  [object]              Name of object to stat.

Optional arguments:
  --lh                  Report sizes in human readable format similar to
                        ls -lh.
'''.strip('\n')


def st_stat(parser, args, output_manager):
    parser.add_option(
        '--lh', dest='human', action='store_true', default=False,
        help='Report sizes in human readable format similar to ls -lh.')
    (options, args) = parse_args(parser, args)
    args = args[1:]

    _opts = vars(options)

    with SwiftService(options=_opts) as swift:
        if not args:
            items, headers = swift.stat()
            output_manager.print_items(items)
            print_account_headers(headers, output_manager)
        else:
            try:
                container = args[0]
                if '/' in container:
                    output_manager.error(
                        'WARNING: / in container name; you might have '
                        'meant %r instead of %r.' %
                        (container.replace('/', ' ', 1), container))
                    return
                args = args[1:]
                if not args:
                    items, headers = swift.stat(container=container)
                    output_manager.print_items(items)
                    print_container_headers(headers, output_manager)
                else:
                    if len(args) == 1:
                        obj = args[0]
                        items, headers = swift.stat(
                            container=container, obj=obj)
                        output_manager.print_items(items, skip_missing=True)
                        print_object_headers(headers, output_manager)
                    else:
                        output_manager.error(
                            'Usage: %s stat %s\n%s', BASENAME,
                            st_stat_options, st_stat_help)

            except SwiftError as e:
                output_manager.error(e.value)


st_post_options = '''[--read-acl <acl>] [--write-acl <acl>] [--sync-to]
                  [--sync-key <sync-key>] [--meta <name:value>]
                  [--header <header>]
                  [container] [object]
'''

st_post_help = '''
Updates meta information for the account, container, or object.
If the container is not found, it will be created automatically.

Positional arguments:
  [container]           Name of container to post to.
  [object]              Name of object to post. Specify multiple times
                        for multiple objects.

Optional arguments:
  --read-acl <acl>      Read ACL for containers. Quick summary of ACL syntax:
                        .r:*, .r:-.example.com, .r:www.example.com, account1,
                        account2:user2
  --write-acl <acl>     Write ACL for containers. Quick summary of ACL syntax:
                        account1 account2:user2
  --sync-to <sync-to>   Sync To for containers, for multi-cluster replication.
  --sync-key <sync-key> Sync Key for containers, for multi-cluster replication.
  --meta <name:value>   Sets a meta data item. This option may be repeated.
                        Example: -m Color:Blue -m Size:Large
  --header <header>     Set request headers. This option may be repeated.
                        Example -H "content-type:text/plain"
'''.strip('\n')


def st_post(parser, args, output_manager):
    parser.add_option(
        '-r', '--read-acl', dest='read_acl', help='Read ACL for containers. '
        'Quick summary of ACL syntax: .r:*, .r:-.example.com, '
        '.r:www.example.com, account1, account2:user2')
    parser.add_option(
        '-w', '--write-acl', dest='write_acl', help='Write ACL for '
        'containers. Quick summary of ACL syntax: account1, '
        'account2:user2')
    parser.add_option(
        '-t', '--sync-to', dest='sync_to', help='Sets the '
        'Sync To for containers, for multi-cluster replication.')
    parser.add_option(
        '-k', '--sync-key', dest='sync_key', help='Sets the '
        'Sync Key for containers, for multi-cluster replication.')
    parser.add_option(
        '-m', '--meta', action='append', dest='meta', default=[],
        help='Sets a meta data item. This option may be repeated. '
        'Example: -m Color:Blue -m Size:Large')
    parser.add_option(
        '-H', '--header', action='append', dest='header',
        default=[], help='Set request headers. This option may be repeated. '
        'Example: -H "content-type:text/plain" '
        '-H "Content-Length: 4000"')
    (options, args) = parse_args(parser, args)
    args = args[1:]
    if (options.read_acl or options.write_acl or options.sync_to or
            options.sync_key) and not args:
        exit('-r, -w, -t, and -k options only allowed for containers')

    _opts = vars(options)

    with SwiftService(options=_opts) as swift:
        try:
            if not args:
                swift.post()
            else:
                container = args[0]
                if '/' in container:
                    output_manager.error(
                        'WARNING: / in container name; you might have '
                        'meant %r instead of %r.' %
                        (args[0].replace('/', ' ', 1), args[0]))
                    return
                args = args[1:]
                if args:
                    if len(args) == 1:
                        obj = args[0]
                        swift.post(container=container, obj=obj)
                    else:
                        output_manager.error(
                            'Usage: %s post %s\n%s', BASENAME,
                            st_post_options, st_post_help)
                else:
                    swift.post(container=container)

        except SwiftError as e:
            output_manager.error(e.value)


st_upload_options = '''[--changed] [--skip-identical] [--segment-size <size>]
                    [--segment-container <container>] [--leave-segments]
                    [--object-threads <thread>] [--segment-threads <threads>]
                    [--header <header>] [--use-slo]
                    [--object-name <object-name>]
                    <container> <file_or_directory>
'''

st_upload_help = '''
Uploads specified files and directories to the given container

Positional arguments:
  <container>           Name of container to upload to.
  <file_or_directory>   Name of file or directory to upload. Specify multiple
                        times for multiple uploads.

Optional arguments:
  --changed             Only upload files that have changed since the last
                        upload.
  --skip-identical      Skip uploading files that are identical on both sides.
  --segment-size <size> Upload files in segments no larger than <size> (in
                        Bytes) and then create a "manifest" file that will
                        download all the segments as if it were the original
                        file.
  --segment-container <container>
                        Upload the segments into the specified container. If
                        not specified, the segments will be uploaded to a
                        <container>_segments container so as to not pollute the
                        main <container> listings.
  --leave-segments      Indicates that you want the older segments of manifest
                        objects left alone (in the case of overwrites).
  --object-threads <threads>
                        Number of threads to use for uploading full objects.
                        Default is 10.
  --segment-threads <threads>
                        Number of threads to use for uploading object segments.
                        Default is 10.
  --header <header>     Set request headers with the syntax header:value.
                        This option may be repeated.
                        Example -H "content-type:text/plain".
  --use-slo             When used in conjunction with --segment-size it will
                        create a Static Large Object instead of the default
                        Dynamic Large Object.
  --object-name <object-name>
                        Upload file and name object to <object-name> or upload
                        dir and use <object-name> as object prefix instead of
                        folder name.
  --no-clobber          Skip uploading files that already exist.
'''.strip('\n')


def st_upload(parser, args, output_manager):
    parser.add_option(
        '-c', '--changed', action='store_true', dest='changed',
        default=False, help='Only upload files that have changed since '
        'the last upload.')
    parser.add_option(
        '--skip-identical', action='store_true', dest='skip_identical',
        default=False, help='Skip uploading files that are identical on '
        'both sides.')
    parser.add_option(
        '-S', '--segment-size', dest='segment_size', help='Upload files '
        'in segments no larger than <size> (in Bytes) and then create a '
        '"manifest" file that will download all the segments as if it were '
        'the original file.')
    parser.add_option(
        '-C', '--segment-container', dest='segment_container',
        help='Upload the segments into the specified container. '
        'If not specified, the segments will be uploaded to a '
        '<container>_segments container so as to not pollute the main '
        '<container> listings.')
    parser.add_option(
        '', '--leave-segments', action='store_true',
        dest='leave_segments', default=False, help='Indicates that you want '
        'the older segments of manifest objects left alone (in the case of '
        'overwrites).')
    parser.add_option(
        '', '--object-threads', type=int, default=10,
        help='Number of threads to use for uploading full objects. '
        'Default is 10.')
    parser.add_option(
        '', '--segment-threads', type=int, default=10,
        help='Number of threads to use for uploading object segments. '
        'Default is 10.')
    parser.add_option(
        '-H', '--header', action='append', dest='header',
        default=[], help='Set request headers with the syntax header:value. '
        ' This option may be repeated. Example -H "content-type:text/plain" '
        '-H "Content-Length: 4000"')
    parser.add_option(
        '', '--use-slo', action='store_true', default=False,
        help='When used in conjunction with --segment-size, it will '
        'create a Static Large Object instead of the default '
        'Dynamic Large Object.')
    parser.add_option(
        '', '--object-name', dest='object_name',
        help='Upload file and name object to <object-name> or upload dir and '
        'use <object-name> as object prefix instead of folder name.')
    parser.add_option(
        '--no-clobber', action='store_true', dest='no_clobber', default=False,
        help='Skip uploading files that already exist.')
    (options, args) = parse_args(parser, args)
    args = args[1:]
    if len(args) < 2:
        output_manager.error(
            'Usage: %s upload %s\n%s', BASENAME, st_upload_options,
            st_upload_help)
        return
    else:
        container = args[0]
        files = args[1:]

    if options.object_name is not None:
        if len(files) > 1:
            output_manager.error('object-name only be used with 1 file or dir')
            return
        else:
            _orig_path = files[0]

    _opts = vars(options)
    _opts['object_uu_threads'] = options.object_threads
    with SwiftService(options=_opts) as swift:
        try:
            _objs = []
            _dir_markers = []
            for f in files:
                if isfile(f):
                    _objs.append(f)
                elif isdir(f):
                    for (_dir, _ds, _fs) in walk(f):
                        if not (_ds + _fs):
                            _dir_markers.append(_dir)
                        else:
                            _objs.extend([join(_dir, _f) for _f in _fs])
                else:
                    output_manager.error("Local file '%s' not found" % f)

            # Now that we've collected all the required files and dir markers
            # build the tuples for the call to upload
            if options.object_name is not None:
                _objs = [
                    (_o, _o.replace(_orig_path, options.object_name, 1))
                    for _o in _objs
                ]
                _dir_markers = [
                    (None, _d.replace(_orig_path, options.object_name, 1),
                     True) for _d in _dir_markers
                ]
            else:
                _objs = zip(_objs, _objs)
                _dir_markers = [(None, _d, True) for _d in _dir_markers]

            for _r in swift.upload(container, _objs + _dir_markers):
                if _r['success']:
                    if options.verbose > 1:
                        if 'attempts' in _r and _r['attempts'] > 1:
                            if 'object' in _r:
                                output_manager.print_msg(
                                    '%s [after %d attempts]' %
                                    (_r['object'],
                                     _r['attempts'])
                                )
                        else:
                            if 'object' in _r:
                                output_manager.print_msg(_r['object'])
                            elif 'for_object' in _r:
                                output_manager.print_msg(
                                    'Segment %s for %s' % (_r['segment_index'],
                                                           _r['for_object'])
                                )
                else:
                    error = _r['error']
                    if isinstance(error, SwiftError):
                        output_manager.error(error.value)
                    else:
                        output_manager.error("Unexpected Error during upload: "
                                             "%s" % error)

        except SwiftError as e:
            output_manager.error(e.value)


st_capabilities_options = "[<proxy_url>]"
st_info_options = st_capabilities_options
st_capabilities_help = '''
Retrieve capability of the proxy

Optional positional arguments:
  <proxy_url>           proxy URL of the cluster to retrieve capabilities
'''
st_info_help = st_capabilities_help


def st_capabilities(parser, args, output_manager):
    def _print_compo_cap(name, capabilities):
        for feature, options in sorted(capabilities.items(),
                                       key=lambda x: x[0]):
            output_manager.print_msg("%s: %s" % (name, feature))
            if options:
                output_manager.print_msg(" Options:")
                for key, value in sorted(options.items(),
                                         key=lambda x: x[0]):
                    output_manager.print_msg("  %s: %s" % (key, value))

    (options, args) = parse_args(parser, args)
    if (args and len(args) > 2):
        output_manager.error('Usage: %s capabilities %s\n%s',
                             BASENAME,
                             st_capabilities_options, st_capabilities_help)
        return

    _opts = vars(options)
    with SwiftService(options=_opts) as swift:
        try:
            if len(args) == 2:
                url = args[1]
                capabilities = swift.capabilities(_opts, url)
            else:
                capabilities = swift.capabilities(_opts)

            _print_compo_cap('Core', {'swift': capabilities['swift']})
            del capabilities['swift']
            _print_compo_cap('Additional middleware', capabilities)

        except SwiftError as e:
            output_manager.error(e.value)

st_info = st_capabilities


def parse_args(parser, args, enforce_requires=True):
    if not args:
        args = ['-h']
    (options, args) = parser.parse_args(args)

    if not (options.auth and options.user and options.key):
        # Use 2.0 auth if none of the old args are present
        options.auth_version = '2.0'

    # Use new-style args if old ones not present
    if not options.auth and options.os_auth_url:
        options.auth = options.os_auth_url
    if not options.user and options.os_username:
        options.user = options.os_username
    if not options.key and options.os_password:
        options.key = options.os_password

    # Specific OpenStack options
    options.os_options = {
        'tenant_id': options.os_tenant_id,
        'tenant_name': options.os_tenant_name,
        'service_type': options.os_service_type,
        'endpoint_type': options.os_endpoint_type,
        'auth_token': options.os_auth_token,
        'object_storage_url': options.os_storage_url,
        'region_name': options.os_region_name,
    }

    if len(args) > 1 and args[0] == "capabilities":
        return options, args

    if (options.os_options.get('object_storage_url') and
            options.os_options.get('auth_token') and
            options.auth_version == '2.0'):
        return options, args

    if enforce_requires and \
            not (options.auth and options.user and options.key):
        exit('''
Auth version 1.0 requires ST_AUTH, ST_USER, and ST_KEY environment variables
to be set or overridden with -A, -U, or -K.

Auth version 2.0 requires OS_AUTH_URL, OS_USERNAME, OS_PASSWORD, and
OS_TENANT_NAME OS_TENANT_ID to be set or overridden with --os-auth-url,
--os-username, --os-password, --os-tenant-name or os-tenant-id. Note:
adding "-V 2" is necessary for this.'''.strip('\n'))
    return options, args


def main(arguments=None):
    if arguments:
        argv = arguments
    else:
        argv = sys_argv

    version = client_version
    parser = OptionParser(version='%%prog %s' % version,
                          usage='''
usage: %prog [--version] [--help] [--snet] [--verbose]
             [--debug] [--info] [--quiet] [--auth <auth_url>]
             [--auth-version <auth_version>] [--user <username>]
             [--key <api_key>] [--retries <num_retries>]
             [--os-username <auth-user-name>] [--os-password <auth-password>]
             [--os-tenant-id <auth-tenant-id>]
             [--os-tenant-name <auth-tenant-name>]
             [--os-auth-url <auth-url>] [--os-auth-token <auth-token>]
             [--os-storage-url <storage-url>] [--os-region-name <region-name>]
             [--os-service-type <service-type>]
             [--os-endpoint-type <endpoint-type>]
             [--os-cacert <ca-certificate>] [--insecure]
             [--no-ssl-compression]
             <subcommand> ...

Command-line interface to the OpenStack Swift API.

Positional arguments:
  <subcommand>
    delete               Delete a container or objects within a container.
    download             Download objects from containers.
    list                 Lists the containers for the account or the objects
                         for a container.
    post                 Updates meta information for the account, container,
                         or object; creates containers if not present.
    stat                 Displays information for the account, container,
                         or object.
    upload               Uploads files or directories to the given container
    capabilities         List cluster capabilities.

Examples:
  %prog -A https://auth.api.rackspacecloud.com/v1.0 -U user -K api_key stat -v

  %prog --os-auth-url https://api.example.com/v2.0 --os-tenant-name tenant \\
      --os-username user --os-password password list

  %prog --os-auth-token 6ee5eb33efad4e45ab46806eac010566 \\
      --os-storage-url https://10.1.5.2:8080/v1/AUTH_ced809b6a4baea7aeab61a \\
      list

  %prog list --lh
'''.strip('\n'))
    parser.add_option('-s', '--snet', action='store_true', dest='snet',
                      default=False, help='Use SERVICENET internal network.')
    parser.add_option('-v', '--verbose', action='count', dest='verbose',
                      default=1, help='Print more info.')
    parser.add_option('--debug', action='store_true', dest='debug',
                      default=False, help='Show the curl commands and results '
                      'of all http queries regardless of result status.')
    parser.add_option('--info', action='store_true', dest='info',
                      default=False, help='Show the curl commands and results '
                      'of all http queries which return an error.')
    parser.add_option('-q', '--quiet', action='store_const', dest='verbose',
                      const=0, default=1, help='Suppress status output.')
    parser.add_option('-A', '--auth', dest='auth',
                      default=environ.get('ST_AUTH'),
                      help='URL for obtaining an auth token.')
    parser.add_option('-V', '--auth-version',
                      dest='auth_version',
                      default=environ.get('ST_AUTH_VERSION', '1.0'),
                      type=str,
                      help='Specify a version for authentication. '
                           'Defaults to 1.0.')
    parser.add_option('-U', '--user', dest='user',
                      default=environ.get('ST_USER'),
                      help='User name for obtaining an auth token.')
    parser.add_option('-K', '--key', dest='key',
                      default=environ.get('ST_KEY'),
                      help='Key for obtaining an auth token.')
    parser.add_option('-R', '--retries', type=int, default=5, dest='retries',
                      help='The number of times to retry a failed connection.')
    parser.add_option('--os-username',
                      metavar='<auth-user-name>',
                      default=environ.get('OS_USERNAME'),
                      help='OpenStack username. Defaults to env[OS_USERNAME].')
    parser.add_option('--os_username',
                      help=SUPPRESS_HELP)
    parser.add_option('--os-password',
                      metavar='<auth-password>',
                      default=environ.get('OS_PASSWORD'),
                      help='OpenStack password. Defaults to env[OS_PASSWORD].')
    parser.add_option('--os_password',
                      help=SUPPRESS_HELP)
    parser.add_option('--os-tenant-id',
                      metavar='<auth-tenant-id>',
                      default=environ.get('OS_TENANT_ID'),
                      help='OpenStack tenant ID. '
                      'Defaults to env[OS_TENANT_ID].')
    parser.add_option('--os_tenant_id',
                      help=SUPPRESS_HELP)
    parser.add_option('--os-tenant-name',
                      metavar='<auth-tenant-name>',
                      default=environ.get('OS_TENANT_NAME'),
                      help='OpenStack tenant name. '
                           'Defaults to env[OS_TENANT_NAME].')
    parser.add_option('--os_tenant_name',
                      help=SUPPRESS_HELP)
    parser.add_option('--os-auth-url',
                      metavar='<auth-url>',
                      default=environ.get('OS_AUTH_URL'),
                      help='OpenStack auth URL. Defaults to env[OS_AUTH_URL].')
    parser.add_option('--os_auth_url',
                      help=SUPPRESS_HELP)
    parser.add_option('--os-auth-token',
                      metavar='<auth-token>',
                      default=environ.get('OS_AUTH_TOKEN'),
                      help='OpenStack token. Defaults to env[OS_AUTH_TOKEN]. '
                           'Used with --os-storage-url to bypass the '
                           'usual username/password authentication.')
    parser.add_option('--os_auth_token',
                      help=SUPPRESS_HELP)
    parser.add_option('--os-storage-url',
                      metavar='<storage-url>',
                      default=environ.get('OS_STORAGE_URL'),
                      help='OpenStack storage URL. '
                           'Defaults to env[OS_STORAGE_URL]. '
                           'Overrides the storage url returned during auth. '
                           'Will bypass authentication when used with '
                           '--os-auth-token.')
    parser.add_option('--os_storage_url',
                      help=SUPPRESS_HELP)
    parser.add_option('--os-region-name',
                      metavar='<region-name>',
                      default=environ.get('OS_REGION_NAME'),
                      help='OpenStack region name. '
                           'Defaults to env[OS_REGION_NAME].')
    parser.add_option('--os_region_name',
                      help=SUPPRESS_HELP)
    parser.add_option('--os-service-type',
                      metavar='<service-type>',
                      default=environ.get('OS_SERVICE_TYPE'),
                      help='OpenStack Service type. '
                           'Defaults to env[OS_SERVICE_TYPE].')
    parser.add_option('--os_service_type',
                      help=SUPPRESS_HELP)
    parser.add_option('--os-endpoint-type',
                      metavar='<endpoint-type>',
                      default=environ.get('OS_ENDPOINT_TYPE'),
                      help='OpenStack Endpoint type. '
                           'Defaults to env[OS_ENDPOINT_TYPE].')
    parser.add_option('--os-cacert',
                      metavar='<ca-certificate>',
                      default=environ.get('OS_CACERT'),
                      help='Specify a CA bundle file to use in verifying a '
                      'TLS (https) server certificate. '
                      'Defaults to env[OS_CACERT].')
    default_val = config_true_value(environ.get('SWIFTCLIENT_INSECURE'))
    parser.add_option('--insecure',
                      action="store_true", dest="insecure",
                      default=default_val,
                      help='Allow swiftclient to access servers without '
                           'having to verify the SSL certificate. '
                           'Defaults to env[SWIFTCLIENT_INSECURE] '
                           '(set to \'true\' to enable).')
    parser.add_option('--no-ssl-compression',
                      action='store_false', dest='ssl_compression',
                      default=True,
                      help='This option is deprecated and not used anymore. '
                           'SSL compression should be disabled by default '
                           'by the system SSL library.')
    parser.disable_interspersed_args()
    (options, args) = parse_args(parser, argv[1:], enforce_requires=False)
    parser.enable_interspersed_args()

    commands = ('delete', 'download', 'list', 'post',
                'stat', 'upload', 'capabilities', 'info')
    if not args or args[0] not in commands:
        parser.print_usage()
        if args:
            exit('no such command: %s' % args[0])
        exit()

    signal.signal(signal.SIGINT, immediate_exit)

    if options.debug or options.info:
        logging.getLogger("swiftclient")
        if options.debug:
            logging.basicConfig(level=logging.DEBUG)
        elif options.info:
            logging.basicConfig(level=logging.INFO)

    had_error = False

    with OutputManager() as output:

        parser.usage = globals()['st_%s_help' % args[0]]
        try:
            globals()['st_%s' % args[0]](parser, argv[1:], output)
        except (ClientException, RequestException, socket.error) as err:
            output.error(str(err))

        had_error = output.error_count

    if had_error:
        exit(1)


if __name__ == '__main__':
    main()
