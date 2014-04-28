# Copyright (c) 2010-2013 OpenStack, LLC.
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
from concurrent.futures import as_completed, CancelledError
from cStringIO import StringIO
from errno import EEXIST, ENOENT
from hashlib import md5
from os import environ, makedirs, utime
from os.path import dirname, exists, getmtime, getsize, isdir, join, \
    sep as os_path_sep
from random import shuffle
from Queue import Queue
from time import time
from threading import Thread
from six.moves.urllib.parse import quote, unquote


try:
    import simplejson as json
except ImportError:
    import json


from swiftclient import Connection
from swiftclient.command_helpers import (
    stat_account, stat_container, stat_object
)
from swiftclient.utils import config_true_value
from swiftclient.exceptions import ClientException
from swiftclient.multithreading import MultiThreadingManager


class SwiftError(Exception):
    def __init__(self, value, container=None, obj=None,
                 segment=None, exc=None):
        self.value = value
        self.container = container
        self.obj = obj
        self.segment = segment
        self.exception = exc

    def __str__(self):
        value = repr(self.value)
        if self.container is not None:
            value += " container:%s" % self.container
        if self.obj is not None:
            value += " object:%s" % self.obj
        if self.segment is not None:
            value += " segment:%s" % self.segment
        return value


def process_options(options):
    if not (options['auth'] and options['user'] and options['key']):
        # Use 2.0 auth if none of the old args are present
        options['auth_version'] = '2.0'

    # Use new-style args if old ones not present
    if not options['auth'] and options['os_auth_url']:
        options['auth'] = options['os_auth_url']
    if not options['user']and options['os_username']:
        options['user'] = options['os_username']
    if not options['key'] and options['os_password']:
        options['key'] = options['os_password']

    # Specific OpenStack options
    options['os_options'] = {
        'tenant_id': options['os_tenant_id'],
        'tenant_name': options['os_tenant_name'],
        'service_type': options['os_service_type'],
        'endpoint_type': options['os_endpoint_type'],
        'auth_token': options['os_auth_token'],
        'object_storage_url': options['os_storage_url'],
        'region_name': options['os_region_name'],
    }


_default_global_options = {
    "snet": False,
    "verbose": 1,
    "debug": False,
    "info": False,
    "auth": environ.get('ST_AUTH'),
    "auth_version": environ.get('ST_AUTH_VERSION', '1.0'),
    "user": environ.get('ST_USER'),
    "key": environ.get('ST_KEY'),
    "retries": 5,
    "os_username": environ.get('OS_USERNAME'),
    "os_password": environ.get('OS_PASSWORD'),
    "os_tenant_id": environ.get('OS_TENANT_ID'),
    "os_tenant_name": environ.get('OS_TENANT_NAME'),
    "os_auth_url": environ.get('OS_AUTH_URL'),
    "os_auth_token": environ.get('OS_AUTH_TOKEN'),
    "os_storage_url": environ.get('OS_STORAGE_URL'),
    "os_region_name": environ.get('OS_REGION_NAME'),
    "os_service_type": environ.get('OS_SERVICE_TYPE'),
    "os_endpoint_type": environ.get('OS_ENDPOINT_TYPE'),
    "os_cacert": environ.get('OS_CACERT'),
    "insecure": config_true_value(environ.get('SWIFTCLIENT_INSECURE')),
    "ssl_compression": False,
    'segment_threads': 10,
    'object_dd_threads': 10,
    'object_uu_threads': 10,
    'container_threads': 10
}


_default_local_options = {
    'sync_to': None,
    'sync_key': None,
    'use_slo': False,
    'segment_size': None,
    'segment_container': None,
    'leave_segments': False,
    'changed': None,
    'skip_identical': False,
    'no_clobber': False,
    'yes_all': False,
    'read_acl': None,
    'write_acl': None,
    'out_file': None,
    'no_download': False,
    'long': False,
    'totals': False,
    'marker': '',
    'header': [],
    'meta': [],
    'prefix': None,
    'delimiter': None,
    'fail_fast': True,
    'human': False
}


def get_conn(options):
    """
    Return a connection building it from the options.
    """
    return Connection(options['auth'],
                      options['user'],
                      options['key'],
                      options['retries'],
                      auth_version=options['auth_version'],
                      os_options=options['os_options'],
                      snet=options['snet'],
                      cacert=options['os_cacert'],
                      insecure=options['insecure'],
                      ssl_compression=options['ssl_compression'])


def mkdirs(path):
    try:
        makedirs(path)
    except OSError as err:
        if err.errno != EEXIST:
            raise


def split_headers(options, prefix=''):
    """
    Splits 'Key: Value' strings and returns them as a dictionary.

    :param options: An array of 'Key: Value' strings
    :param prefix: String to prepend to all of the keys in the dictionary.
        reporting.
    """
    headers = {}
    for item in options:
        split_item = item.split(':', 1)
        if len(split_item) == 2:
            headers[(prefix + split_item[0]).title()] = split_item[1]
        else:
            raise SwiftError("Metadata parameter %s must contain a ':'.\n%s"
                             % (item, "Example: 'Color:Blue' or 'Size:Large'"))
    return headers


class _SwiftReader(object):
    """
    Class for downloading objects from swift and raising appropriate
    errors on failures caused by either invalid md5sum or size of the
    data read.
    """
    def __init__(self, path, body, headers):
        self._path = path
        self._body = body
        self._actual_read = 0
        self._content_length = None
        self._actual_md5 = None
        self._expected_etag = headers.get('etag')

        if 'x-object-manifest' not in headers and \
                'x-static-large-object' not in headers:
            self.actual_md5 = md5()

        if 'content-length' in headers:
            self._content_length = int(headers.get('content-length'))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._actual_md5 is not None:
            etag = self._actual_md5.hexdigest()
            if etag != self._expected_etag:
                raise SwiftError(
                    'Error downloading %s: md5sum != etag, %s != %s' %
                    (self._path, etag, self._expected_etag)
                )

        if self._content_length is not None and \
                self._actual_read != self._content_length:
            raise SwiftError(
                'Error downloading %s: read_length != content_length, '
                '%d != %d' % (self._path, self._actual_read,
                              self._content_length)
            )

    def buffer(self):
        for chunk in self._body:
            if self._actual_md5 is not None:
                self._actual_md5.update(chunk)
            self._actual_read += len(chunk)
            yield chunk

    def bytes_read(self):
        return self._actual_read


class SwiftService(object):
    """
    Service for performing swift operations
    """
    def __init__(self, options=None, timeout=864000):
        self.timeout = timeout
        if options:
            self._options = dict(
                _default_global_options,
                **dict(_default_local_options, **options)
            )
        else:
            self._options = dict(
                _default_global_options,
                **_default_local_options
            )
        process_options(self._options)
        create_connection = lambda: get_conn(self._options)
        self.thread_manager = MultiThreadingManager(
            create_connection,
            segment_threads=self._options['segment_threads'],
            object_dd_threads=self._options['object_dd_threads'],
            object_uu_threads=self._options['object_uu_threads'],
            container_threads=self._options['container_threads']
        )

    def __enter__(self):
        self.thread_manager.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.thread_manager.__exit__(exc_type, exc_val, exc_tb)

    # Stat related methods
    #
    def stat(self, container=None, obj=None, opts=None):
        if opts is not None:
            options = dict(self._options, **opts)
        else:
            options = self._options

        if not container:
            if obj:
                raise SwiftError('stat called on object with no container')
            else:
                try:
                    _stats = self.thread_manager.container_pool.submit(
                        stat_account, options
                    )
                    items, headers = _stats.result(timeout=self.timeout)
                except ClientException as err:
                    if err.http_status != 404:
                        raise err
                    raise SwiftError('Account not found')
        else:
            if not obj:
                try:
                    _stats = self.thread_manager.container_pool.submit(
                        stat_container, options, container
                    )
                    items, headers = _stats.result(timeout=self.timeout)
                except ClientException as err:
                    if err.http_status != 404:
                        raise err
                    raise SwiftError('Container %r not found' % container,
                                     container=container)
            else:
                try:
                    _stats = self.thread_manager.object_dd_pool.submit(
                        stat_object, options, container, obj
                    )
                    items, headers = _stats.result(timeout=self.timeout)
                except ClientException as err:
                    if err.http_status != 404:
                        raise err
                    raise SwiftError(
                        "Object %s/%s not found" % (container, obj),
                        container=container, obj=obj)

        return items, headers

    # Post related methods
    #
    def post(self, container=None, obj=None, options=None):
        if options:
            options = dict(self._options, **options)
        else:
            options = self._options

        _res = {
            'action': 'post',
            'container': container,
            'object': obj,
            'headers': {}
        }
        _response_dict = {}
        if not container:
            if obj:
                raise SwiftError('Object specified, without container')
            else:
                headers = split_headers(
                    options['meta'], 'X-Account-Meta-')
                headers.update(
                    split_headers(options['header'], ''))
                _res['headers'] = headers
                try:
                    _post = self.thread_manager.container_pool.submit(
                        self._post_account_job, headers, _response_dict
                    )
                    _post.result(timeout=self.timeout)
                except ClientException as err:
                    if err.http_status != 404:
                        raise err
                    raise SwiftError('Account not found')
        else:
            if not obj:
                headers = split_headers(
                    options['meta'], 'X-Container-Meta-')
                headers.update(
                    split_headers(options['header'], ''))
                if options['read_acl'] is not None:
                    headers['X-Container-Read'] = options['read_acl']
                if options['write_acl'] is not None:
                    headers['X-Container-Write'] = options['write_acl']
                if options['sync_to'] is not None:
                    headers['X-Container-Sync-To'] = options['sync_to']
                if options['sync_key'] is not None:
                    headers['X-Container-Sync-Key'] = options['sync_key']
                _res['headers'] = headers
                try:
                    _post = self.thread_manager.container_pool.submit(
                        self._post_container_job, container,
                        headers, _response_dict
                    )
                    _post.result(timeout=self.timeout)
                except ClientException as err:
                    if err.http_status != 404:
                        raise
                    raise SwiftError(
                        "Container '%s' not found" % container,
                        container=container
                    )
            else:
                headers = split_headers(
                    options['meta'], 'X-Object-Meta-')
                # add header options to the headers object for the request.
                headers.update(
                    split_headers(options['header'], ''))
                _res['headers'] = headers
                try:
                    _post = self.thread_manager.object_uu_pool.submit(
                        self._post_object_job, container, obj,
                        headers, _response_dict
                    )
                    _post.result(timeout=self.timeout)
                except ClientException as err:
                    if err.http_status != 404:
                        raise
                    raise SwiftError(
                        "Object '%s/%s' not found" % (container, obj),
                        container=container, obj=obj
                    )

        _res['success'] = True
        return _res

    @staticmethod
    def _post_account_job(conn, headers, result):
        return conn.post_account(headers=headers, response_dict=result)

    @staticmethod
    def _post_container_job(conn, container, headers, result):
        try:
            _res = conn.post_container(
                container, headers=headers, response_dict=result)
        except ClientException as err:
            if err.http_status != 404:
                raise
            _r = {}
            _res = conn.put_container(
                container, headers=headers, response_dict=_r)
        return _res

    @staticmethod
    def _post_object_job(conn, container, obj, headers, result):
        return conn.post_object(
            container, obj, headers=headers, response_dict=result)

    # List related methods
    #
    def list(self, container=None, options=None):
        if options:
            options = dict(self._options, **options)
        else:
            options = self._options

        _res = {
            'action': 'list',
            'container': container,
            'prefix': options['prefix']
        }
        try:
            if container is None:
                _listing = self.thread_manager.container_pool.submit(
                    self._list_account_job, options
                )
            else:
                _listing = self.thread_manager.container_pool.submit(
                    self._list_container_job, container, options
                )

            listing = _listing.result(timeout=self.timeout)
            _res.update({
                'success': True,
                'listing': listing
            })
        except ClientException as err:
            if err.http_status != 404:
                raise
            else:
                if not container:
                    raise SwiftError('Account not found')
                else:
                    raise SwiftError('Container %r not found' % container,
                                     container=container)

        return _res

    @staticmethod
    def _list_account_job(conn, options):
        marker = ''
        listing = []
        while True:
            _, _items = conn.get_account(
                marker=marker, prefix=options['prefix']
            )

            if not _items:
                break

            if options['long']:
                for _i in _items:
                    _name = _i['name']
                    _i['meta'] = conn.head_container(_name)

            listing.extend(_items)
            marker = _items[-1].get('name', _items[-1].get('subdir'))

        return listing

    @staticmethod
    def _list_container_job(conn, container, options):
        marker = ''
        listing = []
        while True:
            _, _items = conn.get_container(
                container, marker=marker, prefix=options['prefix'],
                delimiter=options['delimiter']
            )

            if not _items:
                break

            listing.extend(_items)
            marker = _items[-1].get('name', _items[-1].get('subdir'))

        return listing

    # Download related methods
    #
    def download(self, container=None, objects=None, options=None):
        if options:
            _opts = dict(self._options, **options)
        else:
            _opts = self._options

        if not container:
            # Download everything if options['yes_all'] is set
            if _opts['yes_all']:
                try:
                    _opts_c = _opts.copy()
                    _opts_c["long"] = False
                    _containers = [
                        i['name'] for i in
                        self.list(options=_opts_c)["listing"]
                    ]
                    shuffle(_containers)

                    _o_downs = []
                    for _con in _containers:
                        _objs = [
                            i['name'] for i in
                            self.list(
                                container=_con, options=_opts_c)["listing"]
                        ]
                        shuffle(_objs)

                        _o_downs.extend(
                            self.thread_manager.object_dd_pool.submit(
                                self._download_object_job, _con, _obj, _opts_c
                            ) for _obj in _objs
                        )

                    for _o_down in as_completed(_o_downs):
                        yield _o_down.result()

                # If we see a 404 here, the listing of the account failed
                except ClientException as err:
                    if err.http_status != 404:
                        raise
                    raise SwiftError('Account not found')

        elif not objects:
            if '/' in container:
                raise SwiftError('\'/\' in container name',
                                 container=container)
            for _res in self._download_container(container, _opts):
                yield _res

        else:
            if '/' in container:
                raise SwiftError('\'/\' in container name',
                                 container=container)
            if _opts['out_file'] and len(objects) > 1:
                _opts['out_file'] = None

            _o_downs = [
                self.thread_manager.object_dd_pool.submit(
                    self._download_object_job, container, obj, _opts
                ) for obj in objects
            ]

            for _o_down in as_completed(_o_downs):
                yield _o_down.result()

    @staticmethod
    def _download_object_job(conn, container, obj, options):
        out_file = options['out_file']
        _results_dict = {}

        req_headers = split_headers(options['header'], '')

        path = join(container, obj) if options['yes_all'] else obj
        path = path.lstrip(os_path_sep)
        filename = out_file if out_file else path

        if options['no_clobber']:
            if exists(filename):
                # Skip downloading the object
                _res = {
                    'action': 'download_object',
                    'container': container,
                    'object': obj,
                    'success': False,
                    'error': SwiftError(
                        "File %s already exists, skipping." % filename,
                        container=container, obj=obj),
                    'path': filename,
                }
                return _res

        if options['skip_identical'] and out_file != '-':
            try:
                fp = open(filename, 'rb')
            except IOError:
                pass
            else:
                with fp:
                    md5sum = md5()
                    while True:
                        data = fp.read(65536)
                        if not data:
                            break
                        md5sum.update(data)
                    req_headers['If-None-Match'] = md5sum.hexdigest()

        try:
            start_time = time()

            _headers, _body = \
                conn.get_object(container, obj, resp_chunk_size=65536,
                                headers=req_headers,
                                response_dict=_results_dict)
            headers_receipt = time()

            _reader = _SwiftReader(path, _body, _headers)
            with _reader as _obj_body:
                fp = None
                try:
                    make_dir = not options['no_download'] and out_file != "-"
                    content_type = _headers.get('content-type')
                    if content_type.split(';', 1)[0] == 'text/directory':
                        if make_dir and not isdir(path):
                            mkdirs(path)

                        for _ in _obj_body.buffer():
                            continue
                    else:
                        dirpath = dirname(path)
                        if make_dir and dirpath and not isdir(dirpath):
                            mkdirs(dirpath)

                        if not options['no_download']:
                            if out_file == "-":
                                _res = {
                                    'action': 'download_object',
                                    'path': path,
                                    'contents': _obj_body
                                }
                                return _res
                            else:
                                fp = open(filename, 'wb')

                            for chunk in _obj_body.buffer():
                                fp.write(chunk)

                        else:
                            for _ in _obj_body.buffer():
                                continue

                    finish_time = time()
                finally:
                    bytes_read = _obj_body.bytes_read()
                    if fp is not None:
                        fp.close()
                        if 'x-object-meta-mtime' in _headers \
                                and not options['no_download']:
                            mtime = float(_headers['x-object-meta-mtime'])
                            if options['out_file'] \
                                    and not options['out_file'] == "-":
                                utime(options['out_file'], (mtime, mtime))
                            else:
                                utime(path, (mtime, mtime))

            _res = {
                'action': 'download_object',
                'success': True,
                'container': container,
                'object': obj,
                'path': filename,
                'start_time': start_time,
                'finish_time': finish_time,
                'headers_receipt': headers_receipt,
                'auth_end_time': conn.auth_end_time,
                'read_length': bytes_read,
                'attempts': conn.attempts
            }
            return _res

        except Exception as err:
            _res = {
                'action': 'download_object',
                'container': container,
                'object': obj,
                'success': False,
                'error': err,
                'response_dict': _results_dict,
                'path': filename,
                'attempts': conn.attempts
            }
            return _res

    def _download_container(self, container, options):
        try:
            objects = [
                o['name'] for o in
                self.list(container=container, options=options)['listing']
            ]

            _o_downs = [
                self.thread_manager.object_dd_pool.submit(
                    self._download_object_job, container, obj, options
                ) for obj in objects
            ]

            for _o_down in as_completed(_o_downs):
                yield _o_down.result()

        except ClientException as err:
            if err.http_status != 404:
                raise
            raise SwiftError('Container %r not found' % container,
                             container=container)

    # Upload related methods
    #
    def upload(self, container, objs, options=None):
        """
        Upload a list of objects where an object is a tuple containing:

          (file, obj) - A file like object (with a read method) and the
                        name of the object it will be uploaded into.
                        Modified time will be 'now'.
          (path, obj) - A path to a local file and the name of the object
                        it will be uploaded as. Modified time data will be
                        taken from the path.
          (None, obj, dir) - Create an empty object with an mtime of 'now', and
                             create this as a dir marker if dir is True.
        """
        if options:
            options = dict(self._options, **options)
        else:
            options = self._options

        # Does the account exist?
        try:
            self.stat(opts=options)
        except ClientException as err:
            if err.http_status != 404:
                raise
            raise SwiftError('Account not found')

        # Try to create the container, just in case it doesn't exist. If this
        # fails, it might just be because the user doesn't have container PUT
        # permissions, so we'll ignore any error. If there's really a problem,
        # it'll surface on the first object PUT.
        try:
            _create_containers = [
                self.thread_manager.container_pool.submit(
                    self._create_container_job, container
                )
            ]

            if options['segment_size'] is not None:
                seg_container = container + '_segments'
                if options['segment_container']:
                    seg_container = options['segment_container']
                _create_containers.append(
                    self.thread_manager.object_uu_pool.submit(
                        self._create_container_job, seg_container
                    )
                )

            _exceptions = []
            for _r in as_completed(_create_containers):
                try:
                    _res = _r.result(timeout=self.timeout)
                    yield _res
                except Exception as e:
                    _exceptions.append(e)
                    continue

            if _exceptions:
                raise _exceptions[0]

        # Shouldn't raise a PUT failed ClientException here if an object list
        # exists - if the user doesn't have access, all object puts will fail
        except ClientException as err:
            if not objs:
                msg = ' '.join(str(x) for x in (
                    err.http_status, err.http_reason)
                )
                if err.http_response_content:
                    if msg:
                        msg += ': '
                    msg += err.http_response_content[:60]
                raise SwiftError(
                    'Error trying to create container %r: %s' %
                    (container, msg), container=container)

        # We maintain a results queue here and a separate thread to monitor
        # the futures because we want to get results back from potential
        # segment uploads too
        _rq = Queue()
        file_jobs = {}

        for _t in objs:
            _details = {'action': 'upload', 'container': container}
            if hasattr(_t[0], 'read'):
                # We've got a file like object to upload to _o
                (_f, _o) = _t
                _file = self.thread_manager.object_uu_pool.submit(
                    self._upload_object_job, container, _f, _o, options
                )
                _details['file'] = _f
                _details['object'] = _o
                file_jobs[_file] = _details
            elif _t[0] is not None:
                # We've got a path to upload to _o
                (_p, _o) = _t
                _details['path'] = _p
                _details['object'] = _o
                if isdir(_p):
                    _dir = self.thread_manager.object_uu_pool.submit(
                        self._create_dir_marker_job, container, _o,
                        options, path=_p
                    )
                    file_jobs[_dir] = _details
                else:
                    _file = self.thread_manager.object_uu_pool.submit(
                        self._upload_object_job, container, _p, _o,
                        options, results_queue=_rq
                    )
                    file_jobs[_file] = _details
            else:
                # Create an empty object (as a dir marker if is_dir)
                (_, _o, _is_d) = _t
                _details['file'] = None
                _details['object'] = _o
                if _is_d:
                    _dir = self.thread_manager.object_uu_pool.submit(
                        self._create_dir_marker_job, container, _o, options
                    )
                    file_jobs[_dir] = _details
                else:
                    _file = self.thread_manager.object_uu_pool.submit(
                        self._upload_object_job, container, StringIO(),
                        _o, options
                    )
                    file_jobs[_file] = _details

        # Start a thread to watch for upload results
        Thread(
            target=self._watch_futures, args=(file_jobs, _rq)
        ).start()

        # yield results as they become available, including those from
        # segment uploads.
        _res = _rq.get()
        _cancelled = False
        while _res is not None:
            yield _res

            if not _res['success']:
                if not _cancelled and options['fail_fast']:
                    _cancelled = True
                    for _f in file_jobs:
                        _f.cancel()

            _res = _rq.get()

    @staticmethod
    def _create_container_job(conn, container):
        _res = {
            'action': 'create_container',
            'container': container
        }
        _cr = {}
        try:
            conn.put_container(container, response_dict=_cr)
            _res.update({
                'success': True,
                'response_dict': _cr
            })
        except Exception as err:
            _res.update({
                'success': False,
                'error': err,
                'response_dict': _cr
            })
        return _res

    @staticmethod
    def _create_dir_marker_job(conn, container, obj, options, path=None):
        _res = {
            'action': 'create_dir_marker',
            'container': container,
            'object': obj,
            'path': path
        }
        _results_dict = {}
        if obj.startswith('./') or obj.startswith('.\\'):
            obj = obj[2:]
        if obj.startswith('/'):
            obj = obj[1:]
        if path is not None:
            put_headers = {'x-object-meta-mtime': "%f" % getmtime(path)}
        else:
            put_headers = {'x-object-meta-mtime': "%f" % round(time())}
        if options['changed'] or options['no_clobber']:
            try:
                headers = conn.head_object(container, obj)
                # If the head succeeds then the object already exists
                # and the no_clobber option should immediately return
                if options['no_clobber']:
                    _res.update({
                        'success': False,
                        'error': SwiftError(
                            "Object %s already exists, skipping upload." % obj,
                            container=container, obj=obj)})
                    return _res
                ct = headers.get('content-type')
                cl = int(headers.get('content-length'))
                et = headers.get('etag')
                mt = headers.get('x-object-meta-mtime')
                if ct.split(';', 1)[0] == 'text/directory' and \
                        cl == 0 and \
                        et == 'd41d8cd98f00b204e9800998ecf8427e' and \
                        mt == put_headers['x-object-meta-mtime']:
                    _res['success'] = True
                    return _res
            except ClientException as err:
                if err.http_status != 404:
                    _res.update({
                        'success': False,
                        'error': err})
                    return _res
        try:
            conn.put_object(container, obj, '', content_length=0,
                            content_type='text/directory',
                            headers=put_headers,
                            response_dict=_results_dict)
            _res.update({
                'success': True,
                'response_dict': _results_dict})
            return _res
        except Exception as err:
            _res.update({
                'success': False,
                'error': err,
                'response_dict': _results_dict})
            return _res

    @staticmethod
    def _upload_segment_job(conn, path, container, segment_name, segment_start,
                            segment_size, segment_index, obj_name, options,
                            results_queue=None):
        _results_dict = {}
        if options['segment_container']:
            segment_container = options['segment_container']
        else:
            segment_container = container + '_segments'

        _res = {
            'action': 'upload_segment',
            'for_object': obj_name,
            'segment_index': segment_index,
            'segment_size': segment_size,
            'segment_location': '/%s/%s' % (segment_container,
                                            segment_name),
            'log_line': '%s segment %s' % (obj_name, segment_index),
        }
        try:
            fp = open(path, 'rb')
            fp.seek(segment_start)

            etag = conn.put_object(segment_container,
                                   segment_name, fp,
                                   content_length=segment_size,
                                   response_dict=_results_dict)

            _res.update({
                'success': True,
                'response_dict': _results_dict,
                'segment_etag': etag,
                'attempts': conn.attempts
            })

            if results_queue is not None:
                results_queue.put(_res)
            return _res

        except Exception as err:
            _res.update({
                'success': False,
                'error': err,
                'response_dict': _results_dict,
                'attempts': conn.attempts
            })

            if results_queue is not None:
                results_queue.put(_res)
            return _res

    def _upload_object_job(self, conn, container, source, obj, options,
                           results_queue=None):
        _res = {
            'action': 'upload_object',
            'container': container,
            'object': obj
        }
        if hasattr(source, 'read'):
            stream = source
            path = None
        else:
            path = source
        _res['path'] = path
        try:
            if obj.startswith('./') or obj.startswith('.\\'):
                obj = obj[2:]
            if obj.startswith('/'):
                obj = obj[1:]
            if path is not None:
                put_headers = {'x-object-meta-mtime': "%f" % getmtime(path)}
            else:
                put_headers = {'x-object-meta-mtime': "%f" % round(time())}

            # We need to HEAD all objects now in case we're overwriting a
            # manifest object and need to delete the old segments
            # ourselves.
            old_manifest = None
            old_slo_manifest_paths = []
            new_slo_manifest_paths = set()
            if options['changed'] or options['skip_identical'] \
                    or options['no_clobber'] or not options['leave_segments']:
                checksum = None
                if options['skip_identical']:
                    try:
                        fp = open(path, 'rb')
                    except IOError:
                        pass
                    else:
                        with fp:
                            md5sum = md5()
                            while True:
                                data = fp.read(65536)
                                if not data:
                                    break
                                md5sum.update(data)
                        checksum = md5sum.hexdigest()
                try:
                    headers = conn.head_object(container, obj)
                    # If the head succeeds then the object already exists
                    # and the no_clobber option should immediately return
                    if options['no_clobber']:
                        _res.update({
                            'success': False,
                            'error': SwiftError(
                                "Object %s already exists, "
                                    "skipping upload." % obj,
                                container=container, obj=obj)})
                        return _res
                    if options['skip_identical'] and checksum is not None:
                        if checksum == headers.get('etag'):
                            _res.update({
                                'success': True,
                                'status': 'skipped-identical'
                            })
                    cl = int(headers.get('content-length'))
                    mt = headers.get('x-object-meta-mtime')
                    if path is not None and options['changed']\
                            and cl == getsize(path) and \
                            mt == put_headers['x-object-meta-mtime']:
                        _res.update({
                            'success': True,
                            'status': 'skipped-changed'
                        })
                    if not options['leave_segments']:
                        old_manifest = headers.get('x-object-manifest')
                        if config_true_value(
                                headers.get('x-static-large-object')):
                            headers, manifest_data = conn.get_object(
                                container, obj,
                                query_string='multipart-manifest=get'
                            )
                            for old_seg in json.loads(manifest_data):
                                seg_path = old_seg['name'].lstrip('/')
                                if isinstance(seg_path, unicode):
                                    seg_path = seg_path.encode('utf-8')
                                old_slo_manifest_paths.append(seg_path)
                except ClientException as err:
                    if err.http_status != 404:
                        _res.update({
                            'success': False,
                            'error': err
                        })
                        return _res

            # Merge the command line header options to the put_headers
            put_headers.update(split_headers(options['header'], ''))
            # Don't do segment job if object is not big enough, and never do
            # a segment job if we're reading from a stream - we may fail if we
            # go over the single object limit, but this gives us a nice way
            # to create objects from memory
            if path is not None and options['segment_size'] and \
                    getsize(path) > int(options['segment_size']):
                _res['large_object'] = True
                seg_container = container + '_segments'
                if options['segment_container']:
                    seg_container = options['segment_container']
                full_size = getsize(path)

                _segment_futures = []
                segment_pool = self.thread_manager.segment_pool
                segment = 0
                segment_start = 0

                while segment_start < full_size:
                    segment_size = int(options['segment_size'])
                    if segment_start + segment_size > full_size:
                        segment_size = full_size - segment_start
                    if options['use_slo']:
                        segment_name = '%s/slo/%s/%s/%s/%08d' % (
                            obj, put_headers['x-object-meta-mtime'],
                            full_size, options['segment_size'], segment
                        )
                    else:
                        segment_name = '%s/%s/%s/%s/%08d' % (
                            obj, put_headers['x-object-meta-mtime'],
                            full_size, options['segment_size'], segment
                        )
                    _seg = segment_pool.submit(
                        self._upload_segment_job, path, container,
                        segment_name, segment_start, segment_size, segment,
                        obj, options, results_queue=results_queue
                    )
                    _segment_futures.append(_seg)
                    segment += 1
                    segment_start += segment_size

                _segment_results = []
                errors = False
                exceptions = []
                for _f in as_completed(_segment_futures):
                    try:
                        _r = _f.result()
                        if not _r['success']:
                            errors = True
                        _segment_results.append(_r)
                    except Exception as e:
                        errors = True
                        exceptions.append(e)
                if errors:
                    err = ClientException(
                        'Aborting manifest creation '
                        'because not all segments could be uploaded. %s/%s'
                        % (container, obj))
                    _res.update({
                        'success': False,
                        'error': err,
                        'exceptions': exceptions,
                        'segment_results': _segment_results
                    })
                    return _res

                _res['segment_results'] = _segment_results

                if options['use_slo']:
                    _segment_results.sort(key=lambda di: di['segment_index'])
                    for seg in _segment_results:
                        seg_loc = seg['segment_location'].lstrip('/')
                        if isinstance(seg_loc, unicode):
                            seg_loc = seg_loc.encode('utf-8')
                        new_slo_manifest_paths.add(seg_loc)

                    manifest_data = json.dumps([
                        {
                            'path': d['segment_location'],
                            'etag': d['segment_etag'],
                            'size_bytes': d['segment_size']
                        } for d in _segment_results
                    ])

                    put_headers['x-static-large-object'] = 'true'
                    _mr = {}
                    conn.put_object(
                        container, obj, manifest_data,
                        headers=put_headers,
                        query_string='multipart-manifest=put',
                        response_dict=_mr
                    )
                    _res['manifest_response_dict'] = _mr
                else:
                    new_object_manifest = '%s/%s/%s/%s/%s/' % (
                        quote(seg_container), quote(obj),
                        put_headers['x-object-meta-mtime'], full_size,
                        options['segment_size'])
                    if old_manifest and old_manifest.rstrip('/') == \
                            new_object_manifest.rstrip('/'):
                        old_manifest = None
                    put_headers['x-object-manifest'] = new_object_manifest
                    _mr = {}
                    conn.put_object(
                        container, obj, '', content_length=0,
                        headers=put_headers,
                        response_dict=_mr
                    )
                    _res['manifest_response_dict'] = _mr
            else:
                _res['large_object'] = False
                if path is not None:
                    _or = {}
                    conn.put_object(
                        container, obj, open(path, 'rb'),
                        content_length=getsize(path), headers=put_headers,
                        response_dict=_or
                    )
                    _res['response_dict'] = _or
                else:
                    _or = {}
                    conn.put_object(
                        container, obj, stream, headers=put_headers,
                        response_dict=_or
                    )
                    _res['response_dict'] = _or
            if old_manifest or old_slo_manifest_paths:
                if old_manifest:
                    scontainer, sprefix = old_manifest.split('/', 1)
                    scontainer = unquote(scontainer)
                    sprefix = unquote(sprefix).rstrip('/') + '/'
                    delobjs = []
                    for delobj in conn.get_container(scontainer,
                                                     prefix=sprefix)[1]:
                        delobjs.append(delobj['name'])
                    _drs = []
                    for _dr in self.delete(container=scontainer,
                                           objects=delobjs):
                        _drs.append(_dr)
                    _res['segment_delete_results'] = _drs
                if old_slo_manifest_paths:
                    delobjsmap = {}
                    for seg_to_delete in old_slo_manifest_paths:
                        if seg_to_delete in new_slo_manifest_paths:
                            continue
                        scont, sobj = \
                            seg_to_delete.split('/', 1)
                        delobjs_cont = delobjsmap.get(scont, [])
                        delobjs_cont.append(sobj)
                        _drs = []
                        for (dscont, dsobjs) in delobjsmap.items():
                            for _dr in self.delete(container=dscont,
                                                   objects=dsobjs):
                                _drs.append(_dr)
                        _res['segment_delete_results'] = _drs

            # return dict for printing
            _res.update({
                'success': True,
                'status': 'uploaded',
                'attempts': conn.attempts})
            return _res

        except OSError as err:
            if err.errno == ENOENT:
                err = SwiftError('Local file %r not found', path)
            _res.update({
                'success': False,
                'error': err
            })
        except Exception as err:
            _res.update({
                'success': False,
                'error': err
            })
        return _res

    # Delete related methods
    #
    def delete(self, container=None, objects=None, options=None):
        """
        Deletes the given objects, container, or all containers
        """
        if options is not None:
            _opts = dict(self._options, **options)
        else:
            _opts = self._options

        _rq = Queue()
        if container is not None:
            if objects is not None:
                _obj_dels = {}
                for obj in objects:
                    _obj_del = self.thread_manager.object_dd_pool.submit(
                        self._delete_object, container, obj, _opts,
                        results_queue=_rq
                    )
                    _obj_details = {'container': container, 'object': obj}
                    _obj_dels[_obj_del] = _obj_details

                # Start a thread to watch for upload results
                Thread(
                    target=self._watch_futures, args=(_obj_dels, _rq)
                ).start()

                # yield results as they become available, raising the first
                # encountered exception
                _res = _rq.get()
                while _res is not None:
                    yield _res

                    # Cancel the remaining jobs if necessary
                    if _opts['fail_fast'] and not _res['success']:
                        for _d in _obj_dels.keys():
                            _d.cancel()

                    _res = _rq.get()
            else:
                for _res in self._delete_container(container, _opts):
                    yield _res
        else:
            if _opts['yes_all']:
                cancelled = False
                _containers = [
                    _c['name'] for _c in self.list()['listing']
                ]
                for _container in _containers:
                    if cancelled:
                        break
                    else:
                        for _res in self._delete_container(_container,
                                                           options=_opts):
                            yield _res

                            # Cancel the remaining container deletes, but yield
                            # any pending results
                            if not cancelled and \
                                    _opts['fail_fast'] and \
                                    not _res['success']:
                                cancelled = True

    @staticmethod
    def _delete_segment(conn, container, obj, results_queue=None):
        results_dict = {}
        try:
            conn.delete_object(container, obj, response_dict=results_dict)
            _res = {
                'action': 'delete_segment',
                'container': container,
                'object': obj,
                'success': True,
                'attempts': conn.attempts,
                'response_dict': results_dict
            }
        except Exception as e:
            _res = {
                'action': 'delete_segment',
                'container': container,
                'object': obj,
                'success': False,
                'attempts': conn.attempts,
                'response_dict': results_dict,
                'exception': e
            }

        if results_queue is not None:
            results_queue.put(_res)
        return _res

    def _delete_object(self, conn, container, obj, options,
                       results_queue=None):
        try:
            _res = {
                'action': 'delete_object',
                'container': container,
                'object': obj
            }
            old_manifest = None
            query_string = None

            if not options['leave_segments']:
                try:
                    headers = conn.head_object(container, obj)
                    old_manifest = headers.get('x-object-manifest')
                    if config_true_value(
                            headers.get('x-static-large-object')):
                        query_string = 'multipart-manifest=delete'
                except ClientException as err:
                    if err.http_status != 404:
                        raise

            results_dict = {}
            conn.delete_object(container, obj, query_string=query_string,
                               response_dict=results_dict)

            if old_manifest:

                segment_pool = self.thread_manager.segment_pool
                s_container, s_prefix = old_manifest.split('/', 1)
                s_container = unquote(s_container)
                s_prefix = unquote(s_prefix).rstrip('/') + '/'

                _del_segs = []
                _seg_list = self.list(
                    container=s_container, options={'prefix': s_prefix}
                )
                if _seg_list['success']:
                    for _seg in _seg_list['listing']:
                        _del_seg = segment_pool.submit(
                            self._delete_segment, _seg_list['container'],
                            _seg['name'], results_queue=results_queue
                        )
                        _del_segs.append(_del_seg)

                    _seg_results = []
                    for _del_seg in as_completed(_del_segs):
                        _del_res = _del_seg.result()
                        _seg_results.append(_del_res)

                    _res['segment_results'] = _seg_results
                else:
                    _res['segment_results'] = SwiftError(
                        "Failed to list object segments with prefix: "
                        "%s" % s_prefix, container=container, obj=obj
                    )

            _res.update({
                'success': True,
                'response_dict': results_dict,
                'attempts': conn.attempts
            })

        except Exception as err:
            _res['success'] = False
            _res['error'] = err
            return _res

        return _res

    @staticmethod
    def _delete_empty_container(conn, container):
        results_dict = {}
        try:
            conn.delete_container(container, response_dict=results_dict)
            _res = {
                'action': 'delete_container',
                'container': container,
                'object': None,
                'success': True,
                'attempts': conn.attempts,
                'response_dict': results_dict
            }
        except Exception as e:
            _res = {
                'action': 'delete_container',
                'container': container,
                'object': None,
                'success': False,
                'response_dict': results_dict,
                'error': e
            }
        return _res

    def _delete_container(self, container, options):
        _objs = [
            _o['name'] for _o in self.list(container=container)['listing']
        ]

        for _res in self.delete(container=container, objects=_objs,
                                options=options):
            yield _res

        try:
            _con_del = self.thread_manager.container_pool.submit(
                self._delete_empty_container, container
            )
            _con_del_res = _con_del.result()

        except Exception as err:
            _con_del_res = {
                'action': 'delete_container',
                'container': container,
                'object': None,
                'success': False,
                'error': err
            }

        yield _con_del_res

    # Capabilities related methods
    #
    def capabilities(self, options, url=None):
        try:
            _cap = self.thread_manager.container_pool.submit(
                self._get_capabilities, url
            )
            _capabilities = _cap.result()
        except ClientException as err:
            if err.http_status != 404:
                raise err
            raise SwiftError('Account not found')

        return _capabilities

    @staticmethod
    def _get_capabilities(conn, url):
        return conn.get_capabilities(url)

    # Helper methods
    #
    @staticmethod
    def _watch_futures(futures, result_queue):
        """
        Watches a dict of futures and pushes their results onto the given
        queue. We use this to wait for a set of futures which may create
        futures of their own to wait for, whilst also allowing us to
        immediately return the results of those sub-jobs.

        When all futures have completed, None is pushed to the queue

        If the future is cancelled, we use the dict to return details about
        the cancellation.
        """
        for _f in as_completed(futures.keys()):
            try:
                _r = _f.result()
                if _r is not None:
                    result_queue.put(_r)
            except CancelledError:
                _details = futures[_f]
                _res = _details
                _res['status'] = 'cancelled'
                result_queue.put(_res)
            except Exception as err:
                _details = futures[_f]
                _res = _details
                _res['success'] = False
                _res['error'] = err
                result_queue.put(_res)

        result_queue.put(None)
