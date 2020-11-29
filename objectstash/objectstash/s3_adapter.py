import concurrent.futures
import datetime
import math
import pathlib
import shutil
import threading
import time
from timeit import default_timer as timer
import io

import boto3
import botocore
from botocore.client import Config

from .storage_adapter import StorageAdapter


def key_exists(client, bucket, key):
    # Return true if a key exists in s3 bucket
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except botocore.exceptions.ClientError as exc:
        if exc.response['Error']['Code'] != '404':
            raise
        return False
    except:
        raise


def delete_key(client,
               bucket,
               key,
               cache_on_local_disk=True,
               cache_root_path=None,
               verbose=False,
               num_tries=5,
               initial_delay=1.0,
               delay_factor=math.sqrt(2.0)):
    if cache_on_local_disk:
        assert cache_root_path is not None
        cache_root_path = pathlib.Path(cache_root_path).resolve()
        local_filepath = cache_root_path / key
        if local_filepath.is_file():
            local_filepath.unlink()
            if verbose:
                print(f'Removed local cache file {local_filepath}')
    delay = initial_delay
    num_tries_left = num_tries
    while num_tries_left >= 1:
        try:
            client.delete_object(Key=key, Bucket=bucket)
            if verbose:
                print(f'Deleted key {key}')
            return
        except:
            if num_tries_left == 1:
                raise Exception(f'delete backoff failed for key "{key}" at final delay {delay}')
            else:
                time.sleep(delay)
                delay *= delay_factor
                num_tries_left -= 1
    

def get_s3_object_bytes_parallel(keys, *,
                                 client,
                                 client_generator,
                                 bucket,
                                 cache_on_local_disk=True,
                                 cache_root_path=None,
                                 verbose=False,
                                 special_verbose=True,
                                 max_num_threads=90,
                                 num_tries=5,
                                 initial_delay=1.0,
                                 delay_factor=math.sqrt(2.0),
                                 download_callback=None,
                                 skip_modification_time_check=False):
    if client is None:
        assert client_generator is not None
    else:
        assert client_generator is None
        assert max_num_threads <= 1
    if cache_on_local_disk:
        assert cache_root_path is not None
        cache_root_path = pathlib.Path(cache_root_path).resolve()

        missing_keys = []
        existing_keys = []
        for key in keys:
            local_filepath = cache_root_path / key
            if not local_filepath.is_file():
                missing_keys.append(key)
                local_filepath.parent.mkdir(parents=True, exist_ok=True)
            else:
                existing_keys.append(key)

        keys_to_download = missing_keys.copy()
        if skip_modification_time_check:
            if verbose:
                print(f'Skipping the file modification time check for {len(existing_keys)} keys that have local copies.')
            for key in existing_keys:
                if download_callback:
                    download_callback(1)
        else:
            if verbose:
                print(f'Getting metadata for {len(existing_keys)} keys that have local copies ... ', end='')
            metadata_start = timer()
            metadata = get_s3_object_metadata_parallel(existing_keys,
                                                       client=client,
                                                       client_generator=client_generator,
                                                       bucket=bucket,
                                                       verbose=False,
                                                       max_num_threads=max_num_threads,
                                                       num_tries=num_tries,
                                                       initial_delay=initial_delay,
                                                       delay_factor=delay_factor,
                                                       download_callback=None)
            metadata_end = timer()
            if verbose:
                print(f'took {metadata_end - metadata_start:.3f} seconds')
            for key in existing_keys:
                local_filepath = cache_root_path / key
                assert local_filepath.is_file
                local_time = datetime.datetime.fromtimestamp(local_filepath.stat().st_mtime,
                                                             datetime.timezone.utc)
                remote_time = metadata[key]['LastModified']
                # Two second time buffer because S3 modification times are truncated to seconds
                if (remote_time - local_time).total_seconds() >= -2:
                    if verbose:
                        print(f'Local copy of key "{key}" is outdated')
                    keys_to_download.append(key)
                elif download_callback:
                    download_callback(1)

        tl = threading.local()
        def cur_download_file(key):
            local_filepath = cache_root_path / key
            if verbose or special_verbose:
                print('{} not available locally or outdated, downloading from S3 ... '.format(key))
            download_s3_file_with_backoff(key,
                                          str(local_filepath),
                                          client=client,
                                          client_generator=client_generator,
                                          bucket=bucket,
                                          num_tries=num_tries,
                                          initial_delay=initial_delay,
                                          delay_factor=delay_factor,
                                          thread_local=tl)
            return local_filepath.is_file()

        if len(keys_to_download) > 0:
            download_start = timer()
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_num_threads) as executor:
                future_to_key = {executor.submit(cur_download_file, key): key for key in keys_to_download}
                for future in concurrent.futures.as_completed(future_to_key):
                    key = future_to_key[future]
                    try:
                        success = future.result()
                        assert success
                        if download_callback:
                            download_callback(1)
                    except Exception as exc:
                        print('Key {} generated an exception: {}'.format(key, exc))
                        raise exc
            download_end = timer()
            if verbose:
                print('Downloading took {:.3f} seconds'.format(download_end - download_start))

        result = {}
        # TODO: parallelize this as well?
        for key in keys:
            local_filepath = cache_root_path / key
            if verbose:
                print('Reading from local file {} ... '.format(local_filepath), end='')
            with open(local_filepath, 'rb') as f:
                result[key] = f.read()
            if verbose:
                print('done')
    else:
        tl = threading.local()
        def cur_get_object_bytes(key):
            if verbose:
                print('Loading {} from S3 ... '.format(key))
            return get_s3_object_bytes_with_backoff(key,
                                                    client=client,
                                                    client_generator=client_generator,
                                                    bucket=bucket,
                                                    num_tries=num_tries,
                                                    initial_delay=initial_delay,
                                                    delay_factor=delay_factor,
                                                    thread_local=tl)
        download_start = timer()
        result = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_num_threads) as executor:
            future_to_key = {executor.submit(cur_get_object_bytes, key): key for key in keys}
            for future in concurrent.futures.as_completed(future_to_key):
                key = future_to_key[future]
                try:
                    result[key] = future.result()
                    if download_callback:
                        download_callback(1)
                except Exception as exc:
                    print('Key {} generated an exception: {}'.format(key, exc))
                    raise exc
        download_end = timer()
        if verbose:
            print('Getting object bytes took {} seconds'.format(download_end - download_start))
    return result


def get_s3_object_bytes_with_backoff(key, *,
                                     client,
                                     client_generator,
                                     bucket,
                                     num_tries=5,
                                     initial_delay=1.0,
                                     delay_factor=math.sqrt(2.0),
                                     thread_local=None):
    if client is None:
        if thread_local is None:
            client = client_generator()
        else:
            if not hasattr(thread_local, 'get_object_client'):
                thread_local.get_object_client = client_generator()
            client = thread_local.get_object_client
    delay = initial_delay
    num_tries_left = num_tries

    while num_tries_left >= 1:
        try:
            read_bytes = client.get_object(Key=key, Bucket=bucket)["Body"].read()
            return read_bytes
        except:
            if num_tries_left == 1:
                raise Exception('get backoff failed ' + key + ' ' + str(delay))
            else:
                time.sleep(delay)
                delay *= delay_factor
                num_tries_left -= 1


def get_s3_object_metadata_with_backoff(key, *,
                                        client,
                                        client_generator,
                                        bucket,
                                        num_tries=5,
                                        initial_delay=1.0,
                                        delay_factor=math.sqrt(2.0),
                                        thread_local=None):
    if client is None:
        if thread_local is None:
            client = client_generator()
        else:
            if not hasattr(thread_local, 'get_object_client'):
                thread_local.get_object_client = client_generator()
            client = thread_local.get_object_client
    delay = initial_delay
    num_tries_left = num_tries
    while num_tries_left >= 1:
        try:
            metadata = client.head_object(Key=key, Bucket=bucket)
            return metadata
        except:
            if num_tries_left == 1:
                raise Exception('get backoff failed ' + key + ' ' + str(delay))
            else:
                time.sleep(delay)
                delay *= delay_factor
                num_tries_left -= 1


def get_s3_object_metadata_parallel(keys,
                                    client,
                                    client_generator,
                                    bucket,
                                    verbose=False,
                                    max_num_threads=20,
                                    num_tries=5,
                                    initial_delay=1.0,
                                    delay_factor=math.sqrt(2.0),
                                    download_callback=None):
    if client is None:
        assert client_generator is not None
    else:
        assert client_generator is None
        assert max_num_threads <= 1
    tl = threading.local()
    def cur_get_object_metadata(key):
        if verbose:
            print('Loading metadata for {} from S3 ... '.format(key))
        return get_s3_object_metadata_with_backoff(key,
                                                   client=client,
                                                   client_generator=client_generator,
                                                   bucket=bucket,
                                                   num_tries=num_tries,
                                                   initial_delay=initial_delay,
                                                   delay_factor=delay_factor,
                                                   thread_local=tl)
    download_start = timer()
    result = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_num_threads) as executor:
        future_to_key = {executor.submit(cur_get_object_metadata, key): key for key in keys}
        for future in concurrent.futures.as_completed(future_to_key):
            key = future_to_key[future]
            try:
                result[key] = future.result()
                if download_callback:
                    download_callback(1)
            except Exception as exc:
                print('Key {} generated an exception: {}'.format(key, exc))
                raise exc
    download_end = timer()
    if verbose:
        print('Getting object metadata took {} seconds'.format(download_end - download_start))
    return result


def put_s3_object_bytes_with_backoff(file_bytes, key, client, bucket, num_tries=10, initial_delay=1.0, delay_factor=2.0):
    delay = initial_delay
    num_tries_left = num_tries
    while num_tries_left >= 1:
        try:
            bio = io.BytesIO(file_bytes)
            client.upload_fileobj(bio, Key=key, Bucket=bucket, ExtraArgs={'ACL': 'bucket-owner-full-control'})
            return
        except:
            if num_tries_left == 1:
                raise Exception(f'put backoff failed for key {key} ({len(file_bytes)} bytes), last delay {delay}')
            else:
                time.sleep(delay)
                delay *= delay_factor
                num_tries_left -= 1


def list_all_keys(client, bucket, prefix, max_keys=None):
    objects = client.list_objects(Bucket=bucket, Prefix=prefix, Delimiter='/')
    contents = objects.get("Contents", [])
    common_prefixes = objects.get("CommonPrefixes", [])
    keys = list([x['Key'] for x in contents])
    keys += list([x["Prefix"] for x in common_prefixes])
    truncated = objects['IsTruncated']
    next_marker = objects.get('NextMarker')
    while truncated:
        objects = client.list_objects(Bucket=bucket, Prefix=prefix,
                                      Delimiter='/', Marker=next_marker)
        truncated = objects['IsTruncated']
        next_marker = objects.get('NextMarker')
        keys += list(map(lambda x: x['Key'], objects['Contents']))
        if max_keys is not None and len(keys) >= max_keys:
            break
    return list(filter(lambda x: len(x) > 0, keys))


def download_s3_file_with_caching(key, local_filename, *,
                                  bucket,
                                  client,
                                  client_generator,
                                  cache_on_local_disk=True,
                                  cache_root_path=None,
                                  verbose=False,
                                  special_verbose=True,
                                  num_tries=5,
                                  initial_delay=1.0,
                                  delay_factor=math.sqrt(2.0),
                                  skip_modification_time_check=False):
    if client is None:
        assert client_generator is not None
    else:
        assert client_generator is None
    if cache_on_local_disk:
        assert cache_root_path is not None
        cache_root_path = pathlib.Path(cache_root_path).resolve()
        currently_cached = False

        cache_filepath = cache_root_path / key
        if not cache_filepath.is_file():
            cache_filepath.parent.mkdir(parents=True, exist_ok=True)
        else:
            if skip_modification_time_check:
                if verbose:
                    print(f'Skipping the file modification time check the local copy in the cache.')
                currently_cached = True
            else:
                if verbose:
                    print(f'Getting metadata to check the modification time compared to the local copy ... ', end='')
                metadata_start = timer()
                metadata = get_s3_object_metadata_with_backoff(key,
                                                               client=client,
                                                               client_generator=client_generator,
                                                               bucket=bucket,
                                                               num_tries=num_tries,
                                                               initial_delay=initial_delay,
                                                               delay_factor=delay_factor)
                metadata_end = timer()
                if verbose:
                    print(f'took {metadata_end - metadata_start:.3f} seconds')
                local_time = datetime.datetime.fromtimestamp(cache_filepath.stat().st_mtime,
                                                             datetime.timezone.utc)
                remote_time = metadata['LastModified']
                if (remote_time - local_time).total_seconds() >= -2:
                    if verbose:
                        print(f'Local copy of key "{key}" is outdated')
                else:
                    currently_cached = True
        if not currently_cached:
            if verbose or special_verbose:
                print('{} not available locally or outdated, downloading from S3 ... '.format(key))
            download_start = timer()
            download_s3_file_with_backoff(key, str(cache_filepath),
                                          client=client,
                                          client_generator=client_generator,
                                          bucket=bucket,
                                          initial_delay=initial_delay,
                                          delay_factor=delay_factor)
            download_end = timer()
            if verbose:
                print('Downloading took {:.3f} seconds'.format(download_end - download_start))
        assert cache_filepath.is_file()
        if verbose:
            print(f'Copying to the target from the cache file {cache_filepath} ...')
        shutil.copy(cache_filepath, local_filename)
    else:
        if verbose:
            print('Loading {} from S3 ... '.format(key))
        download_start = timer()
        download_s3_file_with_backoff(key, str(local_filename),
                                      client=client,
                                      client_generator=client_generator,
                                      bucket=bucket,
                                      initial_delay=initial_delay,
                                      delay_factor=delay_factor)
        download_end = timer()
        if verbose:
            print('Downloading took {:.3f} seconds'.format(download_end - download_start))


def download_s3_file_with_backoff(key, local_filename, *,
                                  client,
                                  client_generator,
                                  bucket,
                                  num_tries=5,
                                  initial_delay=1.0,
                                  delay_factor=math.sqrt(2.0),
                                  thread_local=None):
    if client is None:
        if thread_local is None:
            client = client_generator()
        else:
            if not hasattr(thread_local, 's3_client'):
                thread_local.s3_client = client_generator()
            client = thread_local.s3_client
    delay = initial_delay
    num_tries_left = num_tries

    while num_tries_left >= 1:
        try:
            client.download_file(bucket, key, local_filename)
            return
        except:
            if num_tries_left == 1:
                raise Exception('download backoff failed ' + ' ' + str(key) + ' ' + str(delay))
            else:
                time.sleep(delay)
                delay *= delay_factor
                num_tries_left -= 1


def upload_file_to_s3_with_backoff(local_filename, key, *,
                                   client,
                                   client_generator,
                                   bucket,
                                   num_tries=5,
                                   initial_delay=1.0,
                                   delay_factor=math.sqrt(2.0),
                                   thread_local=None):
    assert pathlib.Path(local_filename).is_file()
    if client is None:
        if thread_local is None:
            client = client_generator()
        else:
            if not hasattr(thread_local, 's3_client'):
                thread_local.s3_client = client_generator()
            client = thread_local.s3_client
    delay = initial_delay
    num_tries_left = num_tries
    while num_tries_left >= 1:
        try:
            client.upload_file(str(local_filename), bucket, key, ExtraArgs={'ACL': 'bucket-owner-full-control'})
            return
        except:
            if num_tries_left == 1:
                raise Exception('upload backoff failed ' + ' ' + str(key) + ' ' + str(delay))
            else:
                time.sleep(delay)
                delay *= delay_factor
                num_tries_left -= 1


def default_option_if_needed(*, user_option, default):
    if user_option is None:
        return default
    else:
        return user_option


class S3Adapter(StorageAdapter):
    def __init__(self,
                 bucket,
                 profile_name=None,
                 anonymous=False,
                 cache_on_local_disk=False,
                 cache_root_path=None,
                 verbose=False,
                 max_num_threads=64,
                 num_tries=3,
                 initial_delay=1.0,
                 delay_factor=math.sqrt(2.0),
                 skip_modification_time_check=False):
        self.bucket = bucket
        self.cache_on_local_disk = cache_on_local_disk

        if anonymous:
            assert profile_name is None
        if profile_name is not None:
            assert not anonymous
            assert profile_name in boto3.Session()._session.available_profiles
        
        def get_client():
            if anonymous:
                # TODO: test anonymous connections
                session = boto3.Session()
                return session.client('s3', config=Config(signature_version=botocore.UNSIGNED))
            else:
                if profile_name is not None:
                    session = boto3.Session(profile_name=profile_name)
                else:
                    session = boto3.Session()
                return session.client('s3')
        self.get_client = get_client
        self.client = self.get_client()

        if self.cache_on_local_disk:
            assert cache_root_path is not None
            self.cache_root_path = pathlib.Path(cache_root_path).resolve()
            self.cache_root_path.mkdir(parents=True, exist_ok=True)
            assert self.cache_root_path.is_dir()
        else:
            self.cache_root_path = None
        self.verbose = verbose
        self.max_num_threads = max_num_threads
        self.num_tries = num_tries
        self.initial_delay = initial_delay
        self.delay_factor = delay_factor
        self.skip_modification_time_check = skip_modification_time_check

    def list_keys(self, prefix, max_keys=None):
        return list_all_keys(self.client, self.bucket, prefix, max_keys)
    
    def exists(self, key):
        return key_exists(self.client, self.bucket, key)

    def put(self, key, data, verbose=None):
        cur_verbose = default_option_if_needed(user_option=verbose, default=self.verbose)
        put_s3_object_bytes_with_backoff(data,
                                         key,
                                         client=self.client,
                                         bucket=self.bucket,
                                         num_tries=self.num_tries,
                                         initial_delay=self.initial_delay,
                                         delay_factor=self.delay_factor)
        if cur_verbose:
            print(f'Stored {len(data)} bytes under key {key}')

    def put_multiple(self, data_dict, verbose=None):
        # TODO: add a new function for parallel uploading
        # TODO: add a callback parameter
        for key, data in data_dict.items():
            self.put(key, data, verbose)

    def upload_file(self, key, filename, verbose=None):
        upload_file_to_s3_with_backoff(filename,
                                       key,
                                       client=self.client,
                                       client_generator=None,
                                       bucket=self.bucket,
                                       num_tries=self.num_tries,
                                       initial_delay=self.initial_delay,
                                       delay_factor=self.delay_factor,
                                       thread_local=None)
    
    def download_file(self, key, filename, verbose=None, skip_modification_time_check=None):
        cur_verbose = default_option_if_needed(user_option=verbose, default=self.verbose)
        cur_skip_time_check = default_option_if_needed(user_option=skip_modification_time_check,
                                                       default=self.skip_modification_time_check)
        download_s3_file_with_caching(key,
                                      filename,
                                      client=self.client,
                                      client_generator=None,
                                      bucket=self.bucket,
                                      cache_on_local_disk=self.cache_on_local_disk,
                                      cache_root_path=self.cache_root_path,
                                      num_tries=self.num_tries,
                                      initial_delay=self.initial_delay,
                                      delay_factor=self.delay_factor,
                                      skip_modification_time_check=cur_skip_time_check,
                                      verbose=cur_verbose)

    def get(self, key, verbose=None, skip_modification_time_check=None):
        cur_verbose = default_option_if_needed(user_option=verbose, default=self.verbose)
        cur_skip_time_check = default_option_if_needed(user_option=skip_modification_time_check,
                                                       default=self.skip_modification_time_check)
        return get_s3_object_bytes_parallel([key],
                                            client=self.client,
                                            client_generator=None,
                                            bucket=self.bucket,
                                            cache_on_local_disk=self.cache_on_local_disk,
                                            cache_root_path=self.cache_root_path,
                                            verbose=cur_verbose,
                                            max_num_threads=1,
                                            num_tries=self.num_tries,
                                            initial_delay=self.initial_delay,
                                            delay_factor=self.delay_factor,
                                            download_callback=None,
                                            skip_modification_time_check=cur_skip_time_check)[key]

    def get_multiple(self, keys, verbose=None, callback=None, skip_modification_time_check=None):
        cur_verbose = default_option_if_needed(user_option=verbose, default=self.verbose)
        cur_skip_time_check = default_option_if_needed(user_option=skip_modification_time_check,
                                                       default=self.skip_modification_time_check)
        return get_s3_object_bytes_parallel(keys,
                                            client=None,
                                            client_generator=self.get_client,
                                            bucket=self.bucket,
                                            cache_on_local_disk=self.cache_on_local_disk,
                                            cache_root_path=self.cache_root_path,
                                            verbose=cur_verbose,
                                            max_num_threads=self.max_num_threads,
                                            num_tries=self.num_tries,
                                            initial_delay=self.initial_delay,
                                            delay_factor=self.delay_factor,
                                            download_callback=callback,
                                            skip_modification_time_check=cur_skip_time_check)
    
    def delete(self, key, verbose=None):
        cur_verbose = default_option_if_needed(user_option=verbose, default=self.verbose)
        delete_key(self.client,
                   self.bucket,
                   key,
                   cache_on_local_disk=self.cache_on_local_disk,
                   cache_root_path=self.cache_root_path,
                   verbose=cur_verbose,
                   num_tries=self.num_tries,
                   initial_delay=self.initial_delay,
                   delay_factor=self.delay_factor)

    def delete_multiple(self, keys, verbose=None):
        # TODO: add a new function for parallel deletion
        # TODO: add a callback parameter
        for key in keys:
            self.delete_multiple(key)