import pytest
from datetime import datetime
from toolbox.sql.slq_comliance_formatter import DateTimeFormatter


@pytest.mark.parametrize(
    'str_date,date', [
        ('20221013', datetime(2022, 10, 13)),
        ('20220913', datetime(2022, 9, 13)),
        ('20220809', datetime(2022, 8, 9)),
        ('20221109', datetime(2022, 11, 9)),
    ]
)
def test_get_formatted_date(str_date: str, date: datetime):
    assert DateTimeFormatter.get_formatted_date(date) == str_date
