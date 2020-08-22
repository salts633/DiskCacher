import logging
from abc import ABC, abstractmethod
from collections import OrderedDict, Mapping
from contextlib import contextmanager
from pathlib import Path

LOG = logging.getLogger("diskcacher.base")


class FileList(OrderedDict):
    def __setitem__(self, key, value):
        if not isinstance(value, Mapping):
            raise ValueError("New FileList value must be a Mapping")
        if "FilePath" not in value:
            raise ValueError("New FileList value must have a FilePath entry.")
        super().__setitem__(key, value)


class CacheBaseABC(ABC):
    def __init__(self, cache_root, default_open_mode="b", *args, **kwargs):
        self._cache_root = Path(cache_root)
        self._default_open_mode = default_open_mode
        self._open_wa = "w"
        self._file_list = FileList()
        self.get_onDisk_file_list()

    def __contains__(self, item):
        return item in self._file_list

    def reset_cache(self):
        self.empty_cache()
        self.get_onDisk_file_list()

    def empty_cache(self):
        self._file_list = FileList()

    def get_onDisk_file_list(self):
        """Get a list of files on disk and their metadata"""

        files = [f for f in Path(self._cache_root).glob("**/*") if f.is_file()]
        for f in files:
            file_name = str(f.relative_to(self._cache_root))
            self._file_list[file_name] = {"FilePath": f}
            self.update_metadata(file_name, f)

    @abstractmethod
    def cache_oversized(self):
        """Check if the disk cache exceeds some limit"""

    @abstractmethod
    def shrink_cache(self):
        """Remove items from cache until it is below some limit"""

    def update_metadata(self, name, path, *args, **kwargs):
        pass

    @contextmanager
    def append_mode(self):
        old_mode = self._open_wa
        try:
            self._open_wa = "a"
            yield self
        finally:
            self._open_wa = old_mode

    def remove(self, key):
        if key in self._file_list:
            f = self._file_list[key]
            self._file_list.pop(key)
            try:
                LOG.debug("Cacher removing %s", f["FilePath"])
                f["FilePath"].unlink()
            except FileNotFoundError:
                pass

    def __setitem__(self, key, value):
        full_file_path = self._cache_root / Path(key)
        LOG.debug("Cacher making directory", full_file_path.parent)
        full_file_path.parent.mkdir(parents=True, exist_ok=True)

        LOG.debug(
            "opening %s for writing with mode %s",
            full_file_path,
            self._open_wa + self._default_open_mode,
        )
        with open(full_file_path, self._open_wa + self._default_open_mode) as f:
            f.write(value)
        try:
            self._file_list[key].update({"FilePath": full_file_path})
        except KeyError:
            self._file_list[key] = {"FilePath": full_file_path}
        self.update_metadata(key, full_file_path)
        if self.cache_oversized():
            LOG.debug("shrinking cache after setitiem")
            self.shrink_cache(exclude=key)

    def _open_with_mode(self, item, mode):
        if item not in self._file_list:
            raise KeyError(f"{item} not in cache")
        with open(self._file_list[item]["FilePath"], "r" + mode) as f:
            return f

    def __getitem__(self, item):
        return self._open_with_mode(item, self._default_open_mode)
