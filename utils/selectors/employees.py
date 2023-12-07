from datetime import datetime
from toolbox.utils.converters import JSON_to_object, Object_to_JSON, isostr_to_date


def _select(emp: dict) -> dict:
    return {
        'id': emp['id'],
        'friendlyId': emp['friendlyId'],
        'email': emp['email'],
        'chapterId': emp['chapterId'],
        'tribeId': emp['tribeId'],
        'position': emp['position'],
        'levelId': emp['levelId'],
        'locationId': emp['details']['locationId'],
        'name': emp['name'],
        'tribeName': emp['tribeName'],
        'positionName': emp['details']['positionName'],
        'level': emp['details']['level'],
        'hiredAt': emp['details']['hiredAt'],
        'retiredAt': emp['details']['retiredAt'],
        'tents': emp['tents'],
        'roles': emp['details']['roles'],
    }


def _filter(emp: dict, start: datetime) -> dict:
    retired_at = emp['details']['retiredAt']
    return retired_at is None or isostr_to_date(retired_at) > start


def select(emps_json: str, start: str) -> str:
    start = isostr_to_date(start)
    emps = JSON_to_object.convert(emps_json)
    emps = [_select(emp) for emp in emps['page'] if _filter(emp, start)]
    return Object_to_JSON.convert(emps)
