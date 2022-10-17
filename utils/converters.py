import json
from typing import List, Callable, Iterable
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

        JSON_to_DF._convert_dt_columns(df, dt_cols)

        return df

    @staticmethod
    def _convert_dt_columns(df: DataFrame, cols: List[str]):
        for col in cols:
            if col in df.columns:
                df[col] = to_datetime(
                    df[col],
                    utc=False,
                )


class Objects_to_JSON:

    @staticmethod
    def convert(objects: Iterable) -> str:
        str_objects = [str(obj) for obj in objects]
        return json.dumps(str_objects)
