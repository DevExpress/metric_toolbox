import os
import aiohttp
from wrapt import decorator
from collections.abc import Awaitable
from fastapi import status
from fastapi.responses import Response


@decorator
async def with_authorization(func, instance, args, kwargs) -> Awaitable:
    async with aiohttp.ClientSession() as session:
        async with session.get(
            os.environ['AUTH_ENDPOINT'],
            headers={'Authorization': kwargs['access_token']},
        ) as response:
            if response.status == status.HTTP_200_OK:
                return await func(*args, **kwargs)
            resp: Response = kwargs.get('response', None)
            if resp:
                resp.status_code = status.HTTP_403_FORBIDDEN
