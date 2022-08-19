from pandas import DataFrame, read_json


class DF_to_JSON:

    @staticmethod
    def convert(df: DataFrame) -> str:
        return df.to_json(
            orient='records',
            date_format='iso',
        )


class JSON_to_DF:

    @staticmethod
    def convert(json: str) -> DataFrame:
        if not json:
            raise ValueError('Empty response')
        return read_json(
            json,
            orient='records',
        )
