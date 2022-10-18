import json
import numbers
from typing import List, Iterable
from pandas import DataFrame, read_json, to_datetime


class DF_to_JSON:

    @staticmethod
    def convert(df: DataFrame) -> str:
        return df.to_json(
            orient='records',
            date_format='iso',
        )


class JSON_to_DF:

    @staticmethod
    def convert(json: str, dt_cols: List[str] = []) -> DataFrame:
        if not json:
            raise ValueError('Empty response')

        df = read_json(
            json,
            orient='records',
        )

        DateTimeColumnsConverter.convert(df, dt_cols)

        return df


class DateTimeColumnsConverter:

    @staticmethod
    def convert(df: DataFrame, cols: List[str]):
        for col in cols:
            if col in df.columns:
                df[col] = to_datetime(
                    df[col],
                    utc=False,
                )


class Objects_to_JSON:

    @staticmethod
    def convert(objects: Iterable) -> str:
        str_objects = [
            obj if isinstance(obj, numbers.Number) else str(obj)
            for obj in objects
        ]
        return Object_to_JSON.convert(str_objects)


class Object_to_JSON:

    @staticmethod
    def convert(obj) -> str:
        return json.dumps(obj)


class JSON_to_object:

    @staticmethod
    def convert(json_obj: str) -> str:
        return json.loads(json_obj)
