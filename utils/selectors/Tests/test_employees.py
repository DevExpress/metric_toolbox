import pytest
import toolbox.utils.selectors.employees as employees
from toolbox.utils.converters import JSON_to_object


def test_select():
    emps_json = '''{"page": [
            {
                "tribeName": "tribe1",
                "tribeId": "id1",
                "tents": [{"id": "id1", "name": "name1"}],
                "squadName": "squad1",
                "position": "945fde96",
                "levelId": "d3c6d432",
                "chapterId": "29b6e93d",
                "retirementReason": "None",
                "isServiceUser": false,
                "details": {
                    "hiredAt": "2012-10-15T00:00:00",
                    "birthDate": "1991-01-01T00:00:00",
                    "officeNo": "201",
                    "homeAddress": "homeAddress",
                    "canCorrect": false,
                    "level": "6",
                    "lastLevelChanged": null,
                    "nextReview": null,
                    "retiredAt": null,
                    "positionName": "pos",
                    "location": "location",
                    "locationId": "387c5b4d",
                    "activeVacations": null,
                    "photoModificationDate": "0001-01-01T00:00:00",
                    "roles": ["1", "2", "3"]
                },
                "id": "018883f7",
                "friendlyId": "friendlyId1",
                "registerIPAddress": null,
                "registrationDate": "2012-10-15T00:00:00",
                "name": "name1",
                "email": "email1",
                "isEmailInvalid": false,
                "secondaryEmails": null,
                "phone": null,
                "firstName": "firstName1",
                "middleName": null,
                "lastName": "lastName1",
                "isPublicProfile": null,
                "country": null,
                "countryState": null,
                "city": null,
                "company": null,
                "supportEmailNotifications": false,
                "isDisabled": false,
                "canCreateUrgentTicket": false,
                "privilegedOwnedProducts": null,
                "ownerTrackers": null,
                "employeeTrackers": null,
                "userGroups": null,
                "fullName": "fullName1"
        },
        {
                "tribeName": "tribe1",
                "tribeId": "id1",
                "tents": [{"id": "id1", "name": "name1"}],
                "squadName": "squad1",
                "position": "945fde96",
                "levelId": "d3c6d432",
                "chapterId": "29b6e93d",
                "retirementReason": "None",
                "isServiceUser": false,
                "details": {
                    "hiredAt": "2009-10-15T00:00:00",
                    "birthDate": "1991-01-01T00:00:00",
                    "officeNo": "201",
                    "homeAddress": "homeAddress",
                    "canCorrect": false,
                    "level": "6",
                    "lastLevelChanged": null,
                    "nextReview": null,
                    "retiredAt": "2009-12-15T00:00:00",
                    "positionName": "pos",
                    "location": "location",
                    "locationId": "387c5b4d",
                    "activeVacations": null,
                    "photoModificationDate": "0001-01-01T00:00:00",
                    "roles": ["1", "2", "3"]
                },
                "id": "018883f7",
                "friendlyId": "friendlyId1",
                "registerIPAddress": null,
                "registrationDate": "2012-10-15T00:00:00",
                "name": "name1",
                "email": "email1",
                "isEmailInvalid": false,
                "secondaryEmails": null,
                "phone": null,
                "firstName": "firstName1",
                "middleName": null,
                "lastName": "lastName1",
                "isPublicProfile": null,
                "country": null,
                "countryState": null,
                "city": null,
                "company": null,
                "supportEmailNotifications": false,
                "isDisabled": false,
                "canCreateUrgentTicket": false,
                "privilegedOwnedProducts": null,
                "ownerTrackers": null,
                "employeeTrackers": null,
                "userGroups": null,
                "fullName": "fullName1"
        }
        ]}'''
    res = '''[{
            "id": "018883f7",
            "friendlyId": "friendlyId1",
            "email": "email1",
            "chapterId": "29b6e93d",
            "tribeId": "id1",
            "position": "945fde96",
            "levelId": "d3c6d432",
            "locationId": "387c5b4d",
            "name": "name1",
            "tribeName": "tribe1",
            "positionName": "pos",
            "level": "6",
            "hiredAt": "2012-10-15T00:00:00",
            "retiredAt": null,
            "isServiceUser": false,
            "tents": [{"id": "id1", "name": "name1"}],
            "roles": ["1", "2", "3"]
        }]
'''
    assert JSON_to_object.convert(
        employees.select(
            emps_json=emps_json,
            start='2010-01-01',
        )
    ) == JSON_to_object.convert(res)


def test_audit_select():
    emps_audit_json = '''[
    {"entityOid": "a097e533",
        "entityModified": "2023-08-01T12:28:09.33",
        "changedProperties": "Level;Version;ip",
        "tribeId": "474ec3f6",
        "squadId": "00000000",
        "chapterId": "29b6e93d",
        "employeePositionId": "945fde96",
        "employeeLevelId": "e4909108",
        "employeeLocationId": "c2383cb3",
        "hiredAt": "2016-05-11T00:00:00",
        "retiredAt": "0001-01-01T00:00:00"
    },
    {
        "entityOid": "a097e533",
        "entityModified": "2023-08-01T12:28:09.33",
        "changedProperties": "Tribe;Version;ip",
        "tribeId": "474ec3f6",
        "squadId": "00000000",
        "chapterId": "29b6e93d",
        "employeePositionId": "945fde96",
        "employeeLevelId": "e4909108",
        "employeeLocationId": "c2383cb3",
        "hiredAt": "2016-05-11T00:00:00",
        "retiredAt": "0001-01-01T00:00:00"
    },
    {
        "entityOid": "a097e533",
        "entityModified": "2023-08-01T12:28:09.33",
        "changedProperties": "Position;Version;ip",
        "tribeId": "474ec3f6",
        "squadId": "00000000",
        "chapterId": "29b6e93d",
        "employeePositionId": "945fde96",
        "employeeLevelId": "e4909108",
        "employeeLocationId": "c2383cb3",
        "hiredAt": "2016-05-11T00:00:00",
        "retiredAt": "0001-01-01T00:00:00"
    },
    {
        "entityOid": "a097e533",
        "entityModified": "2023-08-01T12:28:09.33",
        "changedProperties": "Location;Version;ip",
        "tribeId": "474ec3f6",
        "squadId": "00000000",
        "chapterId": "29b6e93d",
        "employeePositionId": "945fde96",
        "employeeLevelId": "e4909108",
        "employeeLocationId": "c2383cb3",
        "hiredAt": "2016-05-11T00:00:00",
        "retiredAt": "0001-01-01T00:00:00"
    },
    {
        "entityOid": "a097e533",
        "entityModified": "2023-08-01T12:28:09.33",
        "changedProperties": "Chapter;Version;ip",
        "tribeId": "474ec3f6",
        "squadId": "00000000",
        "chapterId": "29b6e93d",
        "employeePositionId": "945fde96",
        "employeeLevelId": "e4909108",
        "employeeLocationId": "c2383cb3",
        "hiredAt": "2016-05-11T00:00:00",
        "retiredAt": "0001-01-01T00:00:00"
    },
    {
        "entityOid": "a097e533",
        "entityModified": "2023-08-01T12:28:09.33",
        "changedProperties": "Version;ip",
        "tribeId": "474ec3f6",
        "squadId": "00000000",
        "chapterId": "29b6e93d",
        "employeePositionId": "945fde96",
        "employeeLevelId": "e4909108",
        "employeeLocationId": "c2383cb3",
        "hiredAt": "2016-05-11T00:00:00",
        "retiredAt": "0001-01-01T00:00:00"
    }]'''
    res = '''[ {"entityOid": "a097e533",
        "entityModified": "2023-08-01T12:28:09.33",
        "changedProperties": "Level;Version;ip",
        "tribeId": "474ec3f6",
        "chapterId": "29b6e93d",
        "employeePositionId": "945fde96",
        "employeeLevelId": "e4909108",
        "employeeLocationId": "c2383cb3",
        "hiredAt": "2016-05-11T00:00:00",
        "retiredAt": "0001-01-01T00:00:00"
    },
    {
        "entityOid": "a097e533",
        "entityModified": "2023-08-01T12:28:09.33",
        "changedProperties": "Tribe;Version;ip",
        "tribeId": "474ec3f6",
        "chapterId": "29b6e93d",
        "employeePositionId": "945fde96",
        "employeeLevelId": "e4909108",
        "employeeLocationId": "c2383cb3",
        "hiredAt": "2016-05-11T00:00:00",
        "retiredAt": "0001-01-01T00:00:00"
    },
    {
        "entityOid": "a097e533",
        "entityModified": "2023-08-01T12:28:09.33",
        "changedProperties": "Position;Version;ip",
        "tribeId": "474ec3f6",
        "chapterId": "29b6e93d",
        "employeePositionId": "945fde96",
        "employeeLevelId": "e4909108",
        "employeeLocationId": "c2383cb3",
        "hiredAt": "2016-05-11T00:00:00",
        "retiredAt": "0001-01-01T00:00:00"
    },
    {
        "entityOid": "a097e533",
        "entityModified": "2023-08-01T12:28:09.33",
        "changedProperties": "Location;Version;ip",
        "tribeId": "474ec3f6",
        "chapterId": "29b6e93d",
        "employeePositionId": "945fde96",
        "employeeLevelId": "e4909108",
        "employeeLocationId": "c2383cb3",
        "hiredAt": "2016-05-11T00:00:00",
        "retiredAt": "0001-01-01T00:00:00"
    },
    {
        "entityOid": "a097e533",
        "entityModified": "2023-08-01T12:28:09.33",
        "changedProperties": "Chapter;Version;ip",
        "tribeId": "474ec3f6",
        "chapterId": "29b6e93d",
        "employeePositionId": "945fde96",
        "employeeLevelId": "e4909108",
        "employeeLocationId": "c2383cb3",
        "hiredAt": "2016-05-11T00:00:00",
        "retiredAt": "0001-01-01T00:00:00"
    }]'''
    assert employees.audit_select(emps_audit_json=emps_audit_json) == JSON_to_object.convert(res)


@pytest.mark.parametrize(
    'filter, res', [
        (['Position'], '[]'),
        (
            ['Location'], (
                '[{"entityOid": "a097e533",\
"entityModified": "2023-08-01T12:28:09.33",\
"changedProperties": "Location;Version;ip",\
"tribeId": "474ec3f6",\
"chapterId": "29b6e93d",\
"employeePositionId": "945fde96",\
"employeeLevelId": "e4909108",\
"employeeLocationId": "c2383cb3",\
"hiredAt": "2016-05-11T00:00:00",\
"retiredAt": "0001-01-01T00:00:00"}]'
            )
        )
    ]
)
def test_audit_select_filter(filter, res):
    emps_audit_json = '''[{
        "entityOid": "a097e533",
        "entityModified": "2023-08-01T12:28:09.33",
        "changedProperties": "Location;Version;ip",
        "tribeId": "474ec3f6",
        "chapterId": "29b6e93d",
        "employeePositionId": "945fde96",
        "employeeLevelId": "e4909108",
        "employeeLocationId": "c2383cb3",
        "hiredAt": "2016-05-11T00:00:00",
        "retiredAt": "0001-01-01T00:00:00"
    },
    {
        "entityOid": "a097e533",
        "entityModified": "2023-08-01T12:28:09.33",
        "changedProperties": "Chapter;Version;ip",
        "tribeId": "474ec3f6",
        "chapterId": "29b6e93d",
        "employeePositionId": "945fde96",
        "employeeLevelId": "e4909108",
        "employeeLocationId": "c2383cb3",
        "hiredAt": "2016-05-11T00:00:00",
        "retiredAt": "0001-01-01T00:00:00"
    }]'''
    assert employees.audit_select(
        emps_audit_json=emps_audit_json,
        filter_props=filter,
    ) == JSON_to_object.convert(res)
