import pathlib
import shutil
from loguru import logger

from .storage_adapter import StorageAdapter

class FSAdapter(StorageAdapter):
    def __init__(self, rootdir):
        self.rootdir = pathlib.Path(rootdir).resolve()
        if self.rootdir.exists():
            assert self.rootdir.is_dir(), "Root dir must be dir"
        else:
            self.rootdir.mkdir(parents=True)


    def list_keys(self, prefix):
        if len(prefix) == 0:
            prefix = "./"
        elif prefix[0] == "/":
            logger.warning(f"Prefix / not supported for FSAdapter returning []")
            return []
        glob_res = list(self.rootdir.glob(f"{prefix}*"))
        files = [str(x.relative_to(self.rootdir)) for x in glob_res if x.is_file()]
        dirs = [str(x.relative_to(self.rootdir)) + "/" for x in glob_res if x.is_dir()]
        return files + dirs

    def exists(self, key):
        return (self.rootdir / key).exists()

    def put(self, key, data):
        fpath = (self.rootdir / key).resolve()
        assert str(fpath).startswith(str(self.rootdir))
        if not fpath.parent.exists():
            fpath.parent.mkdir(parents=True)
        with fpath.open("wb") as f:
            f.write(data)

    def put_multiple(self, data_dict):
        for key, data in data_dict.items():
            self.put(key, data)

    def upload_file(self, key, filename):
        fpath = (self.rootdir / key).resolve()
        assert str(fpath).startswith(str(self.rootdir))
        shutil.copyfile(filename, fpath)
    
    def get(self, key):
        fpath = (self.rootdir / key).resolve()
        assert str(fpath).startswith(str(self.rootdir))
        with fpath.open("rb") as f:
            return f.read()

    def get_multiple(self, keys):
        ret = {}
        for key in keys:
            ret[key] = self.get(key)
        return ret
    
    def download_file(self, key, filename):
        fpath = (self.rootdir / key).resolve()
        assert str(fpath).startswith(str(self.rootdir))
        shutil.copyfile(fpath, filename)
    
    def delete(self, key):
        fpath = (self.rootdir / key).resolve()
        assert str(fpath).startswith(str(self.rootdir))
        fpath.unlink()

    def delete_multiple(self, keys):
        for key in keys:
            self.delete(key)
