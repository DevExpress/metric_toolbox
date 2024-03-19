import asyncio
import hashlib
from toolbox.cache.async_cache.cache_object import CacheObject


__cache = CacheObject('stat_app_state', expire=None)


async def push_state(state: str):

    def md5(state: str) -> str:
        return hashlib.md5(state.encode()).hexdigest()

    loop = asyncio.get_running_loop()
    state_id = await loop.run_in_executor(None, md5, state)
    
    await __cache.save(value=state, key=[state_id])
    return state_id


async def pull_state(state_id: str):
    return await __cache.get(state_id)
