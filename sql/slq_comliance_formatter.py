from datetime import date


class DateTimeFormatter:

    @staticmethod
    def get_formatted_date(self, date: date) -> str:
        return date.strftime('%Y%m%d')
