from collections.abc import Callable, Iterable
from toolbox.utils.converters import JSON_to_object, Object_to_JSON, isostr_to_date


def _select(
    json: str,
    selector: Callable[[dict], dict],
    filter: Callable[[dict], dict],
    root_selector: Callable[[dict], dict] = lambda obj: obj['page'],
) -> dict:
    json_obj = JSON_to_object.convert(json)
    return [selector(obj) for obj in root_selector(json_obj) if filter(obj)]


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

    def filter(emp: dict) -> bool:
        retired_at = emp['details']['retiredAt']
        return retired_at is None or isostr_to_date(retired_at) > start

    return Object_to_JSON.convert(_select(emps_json, selector, filter))


def audit_select(
    emps_audit_json: str,
    filter_props: Iterable[str] = (
        'Level',
        'Tribe',
        'Position',
        'Location',
        'Chapter',
    )
) -> str:

    def selector(audit: dict) -> dict:
        return {
            'entityOid': audit['entityOid'],
            'entityModified': audit['entityModified'],
            'changedProperties': audit['changedProperties'],
            'chapterId': audit['chapterId'],
            'tribeId': audit['tribeId'],
            'employeePositionId': audit['employeePositionId'],
            'employeeLevelId': audit['employeeLevelId'],
            'employeeLocationId': audit['employeeLocationId'],
            'hiredAt': audit['hiredAt'],
            'retiredAt': audit['retiredAt']
        }

    def filter(audit: dict) -> bool:
        changed_properties: str = audit['changedProperties']
        for prop in filter_props:
            if prop in changed_properties:
                return True
        return False

    def root_selector(audit: dict) -> dict:
        return audit

    return _select(emps_audit_json, selector, filter, root_selector)
