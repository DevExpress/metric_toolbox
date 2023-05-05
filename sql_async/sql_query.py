import asyncio
import aiofiles
from collections.abc import Mapping, Sequence, Callable
from toolbox.utils import json_array_of_objects


class AsyncSqlQuery:
    """
    Represents an interface to an sql query stored on disc.
    """

    def __init__(
        self,
        query_file_path: str,
        fields_mapping: Mapping[str, str],
        fields: Sequence[str],
        format_params: Mapping[str, str] = {},
        formatter: Callable[[Sequence[str], str], str] = json_array_of_objects,
    ) -> None:
        self._file_path = query_file_path
        self.fields_mapping = fields_mapping
        self.fields = fields
        self.format_params = format_params
        self.formatter = formatter

    async def get_script(self) -> str:
        raw_query = await self._read_query_from_file()
        return await asyncio.to_thread(self.format, raw_query)

    def format(self, raw_query: str):
        base_query = raw_query.format(
            **{
                **self.fields_mapping,
                **self.format_params
            }
        )
        return self.formatter(self.fields, base_query)

    def __await__(self):
        return self.get_script().__await__()

    async def _read_query_from_file(self) -> str:
        async with aiofiles.open(self._file_path, encoding='utf-8') as query:
            return await query.read()
