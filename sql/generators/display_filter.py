import asyncio
from collections.abc import Callable
from typing import Any, Protocol, NamedTuple
from toolbox.sql.meta_data import KnotMeta
from toolbox.utils.converters import Object_to_JSON
from toolbox.sql.query_executors.sqlite_query_executor import SQLiteQueryExecutor
from toolbox.sql.sql_query import GeneralSelectSqlQuery
from toolbox.sql.generators import NULL_FILTER_VALUE
from toolbox.sql.generators.filter_clause_generator_factory import (
    BaseNode,
    FilterParametersNode,
    FilterParameterNode,
)


class QueryParams(NamedTuple):
    table: str
    value_field: str = KnotMeta.id
    display_field: str = KnotMeta.name


class DisplayValuesStore(Protocol):

    def get_query_params(field: str) -> QueryParams:
        ...

    def get_display_value(field: str, alias: str, value: Any) -> str:
        ...


async def generate_display_filter(
    node: BaseNode,
    custom_display_filter: Callable[[str, str, Any], list | None],
    display_values_store: DisplayValuesStore,
) -> str:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        None,
        __generate_display_filter_json,
        node,
        custom_display_filter,
        display_values_store,
    )


def __generate_display_filter_json(
    node: BaseNode,
    custom_display_filter: Callable[[str, str, Any], list | None],
    display_values_store: DisplayValuesStore,
):
    filter = __generate_display_filter(node, custom_display_filter, display_values_store)
    return Object_to_JSON.convert(filter)


# yapf: disable
def __generate_display_filter(
    node: BaseNode,
    custom_display_filter: Callable[[str, str, Any], list | None],
    display_values_store: DisplayValuesStore,
) -> list[list]:
    filters = []
    filter_node: BaseNode | FilterParametersNode | FilterParameterNode
    for field_name, filter_node in node.get_field_values().items():
        if filter_node is None:
            continue
        field_alias = node.get_field_alias(field_name)
        filter = custom_display_filter(field_name, field_alias, filter_node)
        if filter is None:
            match filter_node:
                case FilterParametersNode():
                    filter = __generate_filter_from_filter_parameters(
                        field=field_name,
                        alias=field_alias,
                        filter_op=node.get_filter_op(field_name),
                        filter_node=filter_node,
                        display_values_store=display_values_store,
                    )
                    if not filter:
                        continue
                case FilterParameterNode():
                    display_value = display_values_store.get_display_value(
                        field=field_name,
                        alias=field_alias,
                        value=filter_node.value,
                    )
                    filter = [field_alias, '=', display_value]

                case _:
                    filter = __generate_display_filter(node=filter_node)
        __append_filter(filters, filter, node)

    if len(filters) == 1:
        return filters[0]
    return filters


def __generate_filter_from_filter_parameters(
    field: str,
    alias: str,
    filter_op: str,
    filter_node: FilterParametersNode,
    display_values_store: DisplayValuesStore,
):
    if not filter_node.values:
        if not filter_node.include:
            return [alias, '=', 'NULL']
        return ''
    values_contains_null = NULL_FILTER_VALUE in filter_node.values
    values = [value for value in filter_node.values if value != NULL_FILTER_VALUE]

    display_values = __get_display_values(
        field=field,
        values=values,
        display_values_store=display_values_store,
    )

    values_filter = [alias, filter_op, display_values] if display_values else None

    if filter_node.include:
        if values_contains_null:
            return __generate_isnull_fitler(alias, values_filter, '=', 'or')
        return values_filter

    if values_contains_null:
        return __generate_isnull_fitler(alias, values_filter, '!=', 'and')
    return __generate_isnull_fitler(alias, values_filter, '=', 'or')


def __generate_isnull_fitler(alias, values_filter, isnull_op, union_op):
    isnull_filter = [alias, isnull_op, 'NULL']
    if values_filter:
        filter = []
        filter.append(isnull_filter)
        filter.append(union_op)
        filter.append(values_filter)
        return filter
    return isnull_filter


__query_executor = SQLiteQueryExecutor


def __get_display_values(
    field: str,
    values: list,
    display_values_store: DisplayValuesStore,
):
    query_params: QueryParams
    if query_params := display_values_store.get_query_params(field):
        values = ', '.join(f"'{value}'" for value in values)
        return __query_executor().execute(
            main_query=GeneralSelectSqlQuery(
                format_params={
                    'select': query_params.display_field,
                    'from': query_params.table,
                    'where_group_limit':f'WHERE {query_params.value_field} IN ({values})\nGROUP BY {query_params.display_field}',
                }
            )).reset_index(drop=True)[query_params.display_field].values.tolist()
    return values


def __append_filter(filters: list, filter: list, node: BaseNode):
    if not filter:
        return
    if filters:
        filters.append(node.get_append_operator())
    filters.append(filter)
