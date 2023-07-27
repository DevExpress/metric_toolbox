from collections.abc import Sequence
import itertools


def json_array_of_objects(fields: Sequence[str], raw_query: str):
    cols = ',\r\n\t'.join(f"'{v}', {v}" for v in itertools.islice(fields, 0, 63))
    return f"""
SELECT  JSON_GROUP_ARRAY(JSON_OBJECT(
        {cols}
        )) AS json_result
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
