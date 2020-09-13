### class objectstash.ObjectStash(\*\*kwargs)
A simple key-value store that supports different back-ends.


#### delete(key, \*\*kwargs)
Deletes one or multiple keys from the stash.

For instance, stash.delete(key) and stash.delete(keys) both work,
where keys is a list of keys (strings).


* **Parameters**

    **key** (*string** or **list of strings*) – Either a single key or a list of keys.



* **Raises**

    **ValueError** – If the arguments passed in do not match the format above.



* **Returns**

    The function does not return values.



#### download_file(key, filename, \*\*kwargs)
Downloads the data corresponding to the given key into the given file.


* **Parameters**

    
    * **key** (*string*) – The key for which data should be downloaded.


    * **filename** (*string** or **pathlib.Path*) – The target filename.



* **Returns**

    The function does not return values.



#### exists(key, \*\*kwargs)
Checks if a given key exists in the stash.

Eventually this method will also support passing in a list of keys in one call.


* **Parameters**

    **key** (*string*) – The key for which to check whether it exists in the stash.



* **Returns**

    Whether the key exists or not.



* **Return type**

    [bool]



#### get(key, \*\*kwargs)
Retrieves data for one or multiple keys from the stash.

For instance, stash.get(key) and stash.get(keys) both work,
where keys is a list of keys (strings).


* **Parameters**

    **key** (*string** or **list of strings*) – Either a single key or a list of keys.



* **Raises**

    **ValueError** – If the arguments passed in do not match the format above.



* **Returns**

    The data for each key to be retrieved.



* **Return type**

    bytes or dictionary from string to bytes



#### list_keys(prefix, \*\*kwargs)
Lists keys in the stash under the given prefix.

The exact semantics follow the list keys operation from S3.
We will update this method to also return prefixes etc.


* **Parameters**

    **prefix** (*string*) – The prefix under which to list keys.



* **Returns**

    The list of keys in the stash under the prefix.



* **Return type**

    [list of strings]



#### put(key_or_data_dict, \*args, \*\*kwargs)
Inserts one or multiple values into the stash.

For instance, stash.put(key, value) and stash.put(key_value_dict) both work,
where key_value_dict is a dictionary mapping from keys (strings) to data (bytes).


* **Parameters**

    
    * **key_or_data_dict** (*string** or **a dictionary from string to bytes*) – For a single (key, value) pair, the first argument is the key.
    For multiple (key, value) pairs, this is a dictionary from the keys (strings) to the data (bytes) to be inserted.


    * **data** (*bytes*) – If the first argument is a single key (string), the second argument must be the data (bytes) to be inserted.



* **Raises**

    **ValueError** – If the arguments passed in do not match the format above.



* **Returns**

    The function does not return values.



#### upload_file(key, filename, \*\*kwargs)
Uploads a single file given by the filename as data for the given key.

    This function avoids loading the entire file into memory if supported by the back-end adapter.


* **Parameters**

    
    * **key** (*string*) – The target key.


    * **filename** (*string** or **pathlib.Path*) – The file from which the data should be loaded.



* **Returns**

    The function does not return values.
