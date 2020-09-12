import os
import random
import time

import boto3
from moto import mock_s3

from objectstash import __version__, ObjectStash


def test_version():
    assert __version__ == '0.1.0'


def generic_test(stash, tmpdir):
    data1 = b'hello'
    key1 = 'test_key'
    stash.put(key1, data1)
    assert stash.get(key1) == data1

    data2 = bytes(random.getrandbits(8) for _ in range(2000))
    key2 = 'a/test/key'
    stash.put(key2, data2)
    assert stash.get(key2) == data2
    assert stash.get(key1) == data1

    stash.put(key2, data1)
    assert stash.get(key2) == data1
    assert stash.get(key1) == data1
    time.sleep(3)
    assert stash.get(key2) == data1
    assert stash.get(key1) == data1

    key3 = '3'
    data1b = b'world'
    data3 = bytes(random.getrandbits(8) for _ in range(1000))
    stash.put({key1: data1b , key2: data2, key3: data3})
    assert stash.get(key1) == data1b
    assert stash.get(key2) == data2
    assert stash.get(key3) == data3
    
    multi_res = stash.get([key2, key3])
    assert multi_res[key2] == data2
    assert multi_res[key3] == data3
    assert len(multi_res) == 2

    assert stash.exists(key1)
    assert stash.exists(key2)
    assert stash.exists(key3)
    assert not stash.exists('not_a_key')

    stash.delete(key2)
    assert stash.exists(key1)
    assert not stash.exists(key2)
    assert stash.exists(key3)

    stash.put(key2, data3)
    assert stash.exists(key2)
    assert stash.get(key2) == data3

    tmp_filename = tmpdir / 'test1.txt'
    file_content = b'hello objectstash\n'
    with open(tmp_filename, 'wb') as f:
        f.write(file_content)
    stash.upload_file('test1.txt', tmp_filename.resolve())
    assert stash.get('test1.txt') == file_content

    tmp_download_filename = tmpdir / 'test2'
    stash.put(key2, data1)
    stash.download_file(key2, tmp_download_filename)
    with open(tmp_download_filename.resolve(), 'rb') as f:
        tmp_data = f.read()
    assert tmp_data == data1
    time.sleep(3)
    stash.download_file(key2, tmp_download_filename)
    with open(tmp_download_filename.resolve(), 'rb') as f:
        tmp_data = f.read()
    assert tmp_data == data1
    assert set(stash.list_keys('')) == {'3', 'test_key', 'test1.txt', 'a/'}
    assert set(stash.list_keys('/')) == set()
    assert set(stash.list_keys('a/')) == {'a/test/'}
    assert set(stash.list_keys('a/test/')) == {'a/test/key'}
    assert set(stash.list_keys('test')) == {'test_key', 'test1.txt'}

    # Things to test:
    # - exceptions we raise on a non-existing key


def large_parallel_test(stash):
    num_objects = 1000
    data = {}
    for ii in range(num_objects):
        key = str(ii) + '_' + str(random.randint(1000, 1000000))
        size = random.randint(1000, 10000)
        data[key] = bytes(random.getrandbits(8) for _ in range(size))
    stash.put(data)

    res = stash.get(data.keys())
    for key in res:
        assert res[key] == data[key]


def generic_s3_setup(bucket_name='test_bucket'):
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'

    conn = boto3.resource('s3', region_name='us-east-1')
    conn.create_bucket(Bucket=bucket_name)


def test_fs_adapter(tmp_path):
    stash = ObjectStash(rootdir="/tmp/fs_adapter/")
    generic_test(stash, tmp_path)

@mock_s3
def test_s3_adapter_without_local_cache(tmp_path):
    # TODO: add a test that interfaces with real S3
    generic_s3_setup(bucket_name='test_bucket')
    stash = ObjectStash(s3_bucket='test_bucket', cache_on_local_disk=False)
    tmp_data_path = tmp_path / 'tmp_data'
    tmp_data_path.mkdir()
    generic_test(stash, tmp_data_path)


@mock_s3
def test_s3_adapter_parallel_without_local_cache():
    generic_s3_setup(bucket_name='test_bucket')
    stash = ObjectStash(s3_bucket='test_bucket', cache_on_local_disk=False)
    large_parallel_test(stash)


@mock_s3
def test_s3_adapter_with_local_cache(tmp_path):
    # TODO: add a test to make sure caching actually makes something faster?
    #       probably can do that only in a real S3 test
    generic_s3_setup(bucket_name='test_bucket')

    cache_path = tmp_path / 'cache'
    cache_path.mkdir()
    stash = ObjectStash(s3_bucket='test_bucket', cache_root_path=cache_path)
    tmp_data_path = tmp_path / 'tmp_data'
    tmp_data_path.mkdir()
    generic_test(stash, tmp_data_path)


@mock_s3
def test_s3_adapter_parallel_with_local_cache(tmp_path):
    # TODO: add a test to make sure caching actually makes something faster?
    #       probably can do that only in a real S3 test
    generic_s3_setup(bucket_name='test_bucket')

    cache_path = tmp_path / 'cache'
    cache_path.mkdir()
    stash = ObjectStash(s3_bucket='test_bucket', cache_root_path=cache_path)

    large_parallel_test(stash)