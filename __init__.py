from .base import CacheBaseABC
from . import size_strategies
from . import shrink_strategies


class TotalSizeRemoveOldestCache(
    shrink_strategies.RemoveOldestMixin,
    size_strategies.OverallCacheSizeMixin,
    CacheBaseABC,
):
    """Disk cache that limits the overall size on disk by removing old files"""
