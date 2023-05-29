from typing import Any
from pydantic import BaseModel
from pydantic.fields import FieldInfo


class ServerModel(BaseModel):

    def get_field_values(self) -> dict[str, Any]:
        return self.__dict__

    def get_field_alias(self, field_name) -> str:
        return self.__fields__[field_name].alias

    def get_append_operator(self) -> str:
        return 'and'

    def get_filter_op(self, field_name) -> str:
        val = self.__dict__[field_name]
        if hasattr(val, 'include'):
            fi: FieldInfo = self.__fields__[field_name].field_info
            if val.include:
                return fi.extra.get('positive_filter_op', 'in')
            return fi.extra.get('negative_filter_op', 'notin')
        raise NotImplementedError


class FilterNode(ServerModel):
    include: bool


class FilterParameterNode(FilterNode):
    value: int | str


class FilterParametersNode(FilterNode):
    values: list[int | str]


class ViewState(ServerModel):
    state: str
