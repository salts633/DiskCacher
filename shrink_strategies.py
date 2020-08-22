import logging
import pathlib
import warnings
from copy import deepcopy

LOG = logging.getLogger("diskcacher.shrink_strategies")


class _Shrinker:
    def _remove_entries(self, sorted_file_list, exclude=None):
        """Remove entries from the file list until the cache is 'small enough'"""
        LOG.debug("file list %s", sorted_file_list)
        while self.cache_oversized():
            if len(sorted_file_list) < 2:
                warnings.warn("Cache list only 1 entry long: not shrinking cache")
                break
            oldest_name, oldest_details = sorted_file_list[-1]
            if oldest_name != exclude:
                try:
                    self.remove(oldest_name)
                except FileNotFoundError:
                    pass
            sorted_file_list.pop()


class RemoveOldestMixin(_Shrinker):
    def shrink_cache(self, exclude=None):
        """Shrink the cache by removing the oldest files"""

        self._remove_entries(
            sorted(
                self._file_list.items(),
                key=lambda i: i[1]["LastAccessed"],
                reverse=True,
            ),
            exclude=exclude,
        )

    def update_metadata(self, name, path):
        """Make sure last accessed time is in the metadata"""

        last_accessed = path.stat().st_atime
        self._file_list[name].update({"LastAccessed": last_accessed})
        super().update_metadata(name, path)


class RemoveLargestMixin(_Shrinker):
    def shrink_cache(self, exclude=None):
        """Shrink cache by removing the 'largest' files"""

        self._remove_entries(
            sorted(self._file_list.items(), key=lambda i: i[1]["Size"]), exclude=exclude
        )
