import pathlib

MAX_CACHE_SIZE = 1e9  # 1 GB in bytes


class OverallCacheSizeMixin:
    """A Mixin that limits the overall total size of files on disk"""

    def __init__(self, *args, max_cache_size=MAX_CACHE_SIZE, **kwargs):
        self._max_cache_size = max_cache_size
        super().__init__(*args, **kwargs)

    def update_metadata(self, name, path):
        file_size = path.stat().st_size
        self._file_list[name].update({"Size": file_size})
        super().update_metadata(name, path)

    def cache_oversized(self):
        total_size = 0
        for _, file_details in self._file_list.items():
            total_size += file_details["Size"]
        return total_size > self._max_cache_size
