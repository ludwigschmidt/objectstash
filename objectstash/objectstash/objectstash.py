from .s3_adapter import S3Adapter
    

def is_get_list_like(x):
    try:
        iter(x)
        return True
    except:
        return False


class ObjectStash:
    def __init__(self, **kwargs):
        if 's3_bucket' in kwargs:
            bucket = kwargs.pop('s3_bucket')
            self.adapter = S3Adapter(bucket, **kwargs)
        else:
            raise ValueError(f'Currently supported keywords: s3_bucket .')

    # TODO: decide on the semantics for returning keys and / or folders / common prefixes
    def list_keys(self, prefix, **kwargs):
        return self.adapter.list_keys(prefix, **kwargs)

    # TODO: add a version that supports multiple keys? 
    def exists(self, key, **kwargs):
        return self.adapter.exists(key, **kwargs)

    # TODO: add support for passing in file-like objects
    def put(self, key_or_data_dict, *args, **kwargs):
        if type(key_or_data_dict) is dict:
            assert len(args) == 0
            return self.adapter.put_multiple(key_or_data_dict, **kwargs)
        elif type(key_or_data_dict) is str:
            if len(args) == 1 and type(args[0]) is bytes:
                data = args[0]
            else:
                assert len(args) == 0
                assert 'data' in kwargs, f'Must supply data as a positional or keyword argument if a single key is the target.'
                data = kwargs.pop('data')
            return self.adapter.put(key_or_data_dict, data, **kwargs)
        else:
            raise ValueError(f'Unknown data type for data: f{type(data)}. Must be dictionary or bytes.')

    # TODO: add a version that supports multiple keys? 
    def upload_file(self, key, filename , **kwargs):
        return self.adapter.upload_file(key, filename, **kwargs)
    
    def get(self, key, **kwargs):
        if type(key) is str:
            return self.adapter.get(key, **kwargs)
        elif is_get_list_like(key):
            return self.adapter.get_multiple(key, **kwargs)
        else:
            raise ValueError(f'Unknown data type for key: f{type(key)}. Must be string or list.')
    
    # TODO: add a version that supports multiple keys? 
    def download_file(self, key, filename, **kwargs):
        return self.adapter.download_file(key, filename, **kwargs)
    
    # TODO: add a version that supports multiple keys? 
    def delete(self, key, **kwargs):
        if type(key) is str:
            return self.adapter.delete(key, **kwargs)
        elif type(key) is list:
            return self.adapter.delete_multiple(key, **kwargs)
        else:
            raise ValueError(f'Unknown data type for key: f{type(key)}. Must be string or list.')
    