import json
import numbers
from collections.abc import Iterable, Callable
from pathlib import Path
from typing import Any
from datetime import date, datetime
from dateutil.parser import isoparse
from pandas import DataFrame, read_json, to_datetime


class DF_to_JSON:

    @staticmethod
    def convert(df: DataFrame, orient: str = 'records') -> str:
        return df.to_json(
            orient=orient,
            date_format='iso',
        )


class JSON_to_DF:

    @staticmethod
    def convert(
        json: str,
        dt_cols: Iterable[str] = tuple(),
        utc: bool = True,
    ) -> DataFrame:
        if not json:
            raise ValueError('Empty response')

        df = read_json(
            json,
            orient='records',
        )

        DateTimeColumnsConverter.convert(df, dt_cols, utc)

        return df


class DateTimeColumnsConverter:

    @staticmethod
    def convert(
        df: DataFrame,
        cols: Iterable[str],
        utc: bool = True,
        drop_utc: bool = False,
    ):
        for col in cols:
            if col in df.columns:
                df[col] = to_datetime(
                    df[col],
                    utc=utc,
                )
                if drop_utc:
                    df[col] = df[col].dt.tz_localize(None)

    @staticmethod
    def convert_many(
        dfs: Iterable[DataFrame],
        cols: Iterable[str],
        utc: bool = True,
        drop_utc: bool = False
    ):
        for df in dfs:
            DateTimeColumnsConverter.convert(
                df=df,
                cols=cols,
                utc=utc,
                drop_utc=drop_utc,
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
    def convert(json_obj: str) -> Any:
        return json.loads(json_obj)


class DateTimeToSqlString:

    @staticmethod
    def convert(date: date, separator='') -> str:
        return date.strftime(f'%Y{separator}%m{separator}%d')

    @staticmethod
    def convert_from_utcstring(dt: str, separator='') -> str:
        return DateTimeToSqlString.convert(
            date=datetime.strptime(dt, '%Y-%m-%dT%H:%M:%SZ'),
            separator='-',
        )


def isostr_to_date(dt: str) -> datetime:
    return isoparse(dt)


def to_quoted_string(val) -> str:
    return f"'{val}'"


def file_to_dict(
    file: Path,
    title_converter: Callable[[str], str] = lambda x: x,
) -> dict:
    desc = {
        'title': '',
        'content': '',
    }
    if file.exists():
        desc['title'] = title_converter(file.stem)
        desc['content'] = file.read_text()
    return desc
