import requests
import urllib3


urllib3.disable_warnings()


def get_data(
    end_point: str,
    headers={},
    params={},
) -> str:
    resp = requests.get(
        url=end_point,
        headers=headers,
        params=params,
        verify=False,
    )
    _raise_if_status_code_indicates_failure(
        end_point=end_point,
        status_code=resp.status_code,
        success_status_code=200,
    )
    return resp.text


def post_data(end_point: str, data: str):
    resp = requests.post(url=end_point, json=data)
    _raise_if_status_code_indicates_failure(
        end_point=end_point,
        status_code=resp.status_code,
        success_status_code=201,
    )
    return resp.status_code


def _raise_if_status_code_indicates_failure(
    end_point: str,
    status_code: int,
    success_status_code: int = 200,
):
    if status_code != success_status_code:
        raise RequestFailure(end_point)


class RequestFailure(Exception):
    pass
