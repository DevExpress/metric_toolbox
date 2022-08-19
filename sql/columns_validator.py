from pandas import DataFrame
from typing import Iterable


def ensure_must_have_columns(
    df: DataFrame,
    must_have_columns: Iterable[str],
) -> None:
    if not all(col_name in df.columns for col_name in must_have_columns):
        raise InvalidDataFormatException(must_have_columns, df.columns)


class InvalidDataFormatException(Exception):
    """
    Is thrown when the DataFrame doesn't contain required columns.
    """

    def __init__(
        self,
        expected_column_names: Iterable[str],
        actual_column_names: Iterable[str],
    ) -> None:
        expected_column_names = sorted(expected_column_names)
        self.message = f'DataFrame must contain ({", ".join(expected_column_names)}) but got ({", ".join(actual_column_names)})'
        Exception.__init__(self, self.message)
