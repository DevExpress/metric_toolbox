from collections.abc import Sequence
import itertools


def json_array_of_objects(fields: Sequence[str], raw_query: str):
    SQLITE_MAX_FUNCTION_ARG = 63
    cols = ',\n\t'.join(f"'{v}', {v}" for v in itertools.islice(fields, 0, SQLITE_MAX_FUNCTION_ARG))
    json_obj_fn = f'JSON_OBJECT(\n\t{cols})'
    for v in itertools.islice(fields, SQLITE_MAX_FUNCTION_ARG, None):
        json_obj_fn = f"JSON_INSERT(\n\t{json_obj_fn},\n\t '$.{v}', {v})"
    return f"""
SELECT  JSON_GROUP_ARRAY(
        {json_obj_fn}
    ) AS json_result
FROM (
{raw_query}
)"""


def json_array_of_values(fields: Sequence[str], raw_query: str):
    return f"""
SELECT  JSON_GROUP_ARRAY(
        {fields[0]}
        ) AS json_result
FROM (
{raw_query}
)"""
