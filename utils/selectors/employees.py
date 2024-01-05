from collections.abc import Callable
from toolbox.utils.converters import JSON_to_object, Object_to_JSON, isostr_to_date


def _select(
    json: str,
    selector: Callable[[dict], dict],
    filter: Callable[[dict], dict],
) -> str:
    json_obj = JSON_to_object.convert(json)
    res = [selector(obj) for obj in json_obj['page'] if filter(obj)]
    return Object_to_JSON.convert(res)


def select(emps_json: str, start: str) -> str:
    def selector(emp: dict) -> dict:
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
            'isServiceUser': emp['isServiceUser'],
            'tents': emp['tents'],
            'roles': emp['details']['roles'],
        }

    start = isostr_to_date(start)

    def filter(emp: dict) -> dict:
        retired_at = emp['details']['retiredAt']
        return retired_at is None or isostr_to_date(retired_at) > start

    return _select(emps_json, selector, filter)
