import hashlib
from toolbox.cache.cache_object import CacheObject


__cache = CacheObject('stat_app_state', expire=None)


def push_state(state: str):
    state_id = hashlib.md5(state.encode()).hexdigest()
    __cache.save(value=state, key=[state_id])
    return state_id


def pull_state(state_id: str):
    return __cache.get(state_id)
