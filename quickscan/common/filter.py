from typing import Any
import logging

logger = logging.getLogger(__name__)


class ObjectFilter:

    def __init__(self, kv: str):
        self._kv = kv

        self.filters = {}
        self._build_filter()

    @property
    def valid(self):
        return not self.filters == {}

    def _build_filter(self):
        for kv_pair in self._kv.split(','):
            assert "=" in kv_pair
            k, v = kv_pair.split('=')
            assert k
            assert v
            if v.upper() in ['TRUE', 'FALSE']:
                v = v.upper() in ['TRUE', 'FALSE']
            self.filters[k] = v

    def ok(self, obj: Any):
        for k, v in self.filters.items():
            key = k
            subkey = None
            if '/' in k:
                key, subkey = k.split('/')
            if hasattr(obj, key):
                if subkey:
                    a = getattr(obj, key)
                    if not isinstance(a, dict):
                        logger.debug(f'filter skipping {k}: not a dict')
                        continue
                    val = a.get(subkey, None)
                else:
                    val = getattr(obj, key, None)
                if val != v:
                    logger.debug(f'filter result - no match on {k}: {val} != {v}')
                    return False
            else:
                logger.debug(f'filter key ({k}) not found in the object. Key ignored.')
        return True
