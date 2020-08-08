from abc import ABC, abstractmethod
 
class StorageAdapter(ABC):
    @abstractmethod
    def list_keys(self, prefix, **kwargs):
        pass
    
    @abstractmethod
    def exists(self, key, **kwargs):
        pass

    @abstractmethod
    def put(self, key, data, **kwargs):
        pass

    @abstractmethod
    def put_multiple(self, data_dict, **kwargs):
        pass

    @abstractmethod
    def upload_file(self, key, filename, **kwargs):
        pass
    
    @abstractmethod
    def get(self, key, **kwargs):
        pass

    @abstractmethod
    def get_multiple(self, keys, **kwargs):
        pass
    
    @abstractmethod
    def download_file(self, key, filename, **kwargs):
        pass
    
    @abstractmethod
    def delete(self, key, **kwargs):
        pass

    @abstractmethod
    def delete_multiple(self, keys, **kwargs):
        pass