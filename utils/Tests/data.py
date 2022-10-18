df_data = {
    'ds':
        [
            '2022-04-17 04:00:00.000',
            '2022-04-18 00:00:00.000',
            '2022-04-19 01:00:00.000',
        ],
    'y': [1, 2, 3]
}


class TstClass:

    def __init__(self, abbr: str):
        self.abbr = abbr

    def __str__(self) -> str:
        return self.abbr
