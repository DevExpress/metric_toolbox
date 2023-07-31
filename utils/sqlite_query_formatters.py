from collections.abc import Sequence
import itertools
import math


def json_array_of_objects(fields: Sequence[str], raw_query: str):
    return f"""
SELECT  JSON_GROUP_ARRAY(
        {_json_object(fields)}
    ) AS json_result
FROM (
{raw_query}
)"""


# yapf: disable
def _json_object(fields: Sequence[str]) -> tuple[int, str]:
    SQLITE_MAX_FUNCTION_ARG = 63
    parts = math.ceil(len(fields) / SQLITE_MAX_FUNCTION_ARG)

    def gen():
        for i in range(parts):
            start = i * SQLITE_MAX_FUNCTION_ARG
            cols = ',\n\t'.join(f"'{v}', {v}" for v in itertools.islice(fields, start, start + SQLITE_MAX_FUNCTION_ARG))
            yield f'JSON_OBJECT(\n\t{cols})'

    json_obj_fn = ',\n\t'.join(gen())
    if parts > 1:
        return f'JSON_PATCH(\n\t{json_obj_fn})'
    return json_obj_fn
# yapf: enable


def json_array_of_values(fields: Sequence[str], raw_query: str):
    return f"""
SELECT  JSON_GROUP_ARRAY(
        {fields[0]}
        ) AS json_result
FROM (
{raw_query}
)"""
