from .s3_adapter import S3Adapter
from .fs_adapter import FSAdapter
    

def is_get_list_like(x):
    try:
        iter(x)
        return True
    except:
        return False


class ObjectStash:
    """A simple key-value store that supports different back-ends.
    """    
    def __init__(self, **kwargs):
        """Constructs an object stash with back-end depending on the keyword arguments passed in.

        If one keyword is "s3_bucket", constructs an S3-based object stash for the given bucket. The S3 back-end
            currently supports the following options: TODO: document this.

        Raises:
            ValueError: If the keyword arguments do not contain a recognized keyword that determines the back-end.
        """        
        if 's3_bucket' in kwargs:
            bucket = kwargs.pop('s3_bucket')
            self.adapter = S3Adapter(bucket, **kwargs)
        elif 'rootdir' in kwargs:
            rootdir = kwargs.pop('rootdir')
            self.adapter = FSAdapter(rootdir)
        else:
            raise ValueError(f'Currently supported keywords: s3_bucket .')

    def list_keys(self, prefix, **kwargs):
        """Lists keys in the stash under the given prefix.

        The exact semantics follow the list keys operation from S3.
        We will update this method to also return prefixes etc.

        Args:
            prefix (string): The prefix under which to list keys.

        Returns:
            [list of strings]: The list of keys in the stash under the prefix.
        """        
        return self.adapter.list_keys(prefix, **kwargs)

    # TODO: add a version that supports multiple keys? 
    def exists(self, key, **kwargs):
        """Checks if a given key exists in the stash.

        Eventually this method will also support passing in a list of keys in one call.

        Args:
            key (string): The key for which to check whether it exists in the stash.

        Returns:
            [bool]: Whether the key exists or not.
        """        
        return self.adapter.exists(key, **kwargs)

    # TODO: add support for passing in file-like objects
    def put(self, key_or_data_dict, *args, **kwargs):
        """Inserts one or multiple values into the stash.

        For instance, stash.put(key, value) and stash.put(key_value_dict) both work,
        where key_value_dict is a dictionary mapping from keys (strings) to data (bytes).

        Args:
            key_or_data_dict (string or a dictionary from string to bytes): For a single (key, value) pair, the first argument is the key.
                    For multiple (key, value) pairs, this is a dictionary from the keys (strings) to the data (bytes) to be inserted.
            data (bytes): If the first argument is a single key (string), the second argument must be the data (bytes) to be inserted.

        Raises:
            ValueError: If the arguments passed in do not match the format above.

        Returns:
            The function does not return values.
        """        
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
        """Uploads a single file given by the filename as data for the given key.
           This function avoids loading the entire file into memory if supported by the back-end adapter.

        Args:
            key (string): The target key.
            filename (string or pathlib.Path): The file from which the data should be loaded.

        Returns:
            The function does not return values.
        """        
        return self.adapter.upload_file(key, filename, **kwargs)
    
    def get(self, key, **kwargs):
        """Retrieves data for one or multiple keys from the stash.

        For instance, stash.get(key) and stash.get(keys) both work,
        where keys is a list of keys (strings).

        Args:
            key (string or list of strings): Either a single key or a list of keys.

        Raises:
            ValueError: If the arguments passed in do not match the format above.

        Returns:
            bytes or dictionary from string to bytes: The data for each key to be retrieved.
        """        
        if type(key) is str:
            return self.adapter.get(key, **kwargs)
        elif is_get_list_like(key):
            return self.adapter.get_multiple(key, **kwargs)
        else:
            raise ValueError(f'Unknown data type for key: f{type(key)}. Must be string or list.')
    
    # TODO: add a version that supports multiple keys? 
    def download_file(self, key, filename, **kwargs):
        """Downloads the data corresponding to the given key into the given file.

        Args:
            key (string): The key for which data should be downloaded.
            filename (string or pathlib.Path): The target filename.

        Returns:
            The function does not return values.
        """        
        return self.adapter.download_file(key, filename, **kwargs)
    
    def delete(self, key, **kwargs):
        """Deletes one or multiple keys from the stash.
        
        For instance, stash.delete(key) and stash.delete(keys) both work,
        where keys is a list of keys (strings).

        Args:
            key (string or list of strings): Either a single key or a list of keys.

        Raises:
            ValueError: If the arguments passed in do not match the format above.

        Returns:
            The function does not return values.
        """        
        if type(key) is str:
            return self.adapter.delete(key, **kwargs)
        elif type(key) is list:
            return self.adapter.delete_multiple(key, **kwargs)
        else:
            raise ValueError(f'Unknown data type for key: f{type(key)}. Must be string or list.')
    