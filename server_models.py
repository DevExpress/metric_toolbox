from typing import Any, Generic, TypeVar
from pydantic.fields import FieldInfo
from pydantic.generics import GenericModel
from toolbox.sql.generators import null_filter_type, in_, notin, eq, ne


T = TypeVar('T', int, str, bool, float, null_filter_type)


class ServerModel(GenericModel):

    def get_field_values(self) -> dict[str, Any]:
        return self.__dict__

    def get_field_alias(self, field_name) -> str:
        return self.__fields__[field_name].alias

    def get_append_operator(self) -> str:
        return 'and'

    def get_filter_op(self, field_name) -> str:
        val = self.__dict__[field_name]
        if hasattr(val, 'include'):
            hasValues = hasattr(val, 'values')
            fi: FieldInfo = self.__fields__[field_name].field_info
            if val.include:
                return fi.extra.get('positive_filter_op', in_ if hasValues else eq)
            return fi.extra.get('negative_filter_op', notin if hasValues else ne)
        raise NotImplementedError


class FilterNode(ServerModel):
    include: bool

    def __bool__(self):
        return self.include


class FilterParameterNode(FilterNode, Generic[T]):
    value: T


class FilterParametersNode(FilterNode, Generic[T]):
    values: list[T]


class ViewState(ServerModel):
    state: str
