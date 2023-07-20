import pytest
import toolbox.sql.generators.display_filter as DisplayFilterGenerator
from pandas import DataFrame
from pydantic import Field
from toolbox.sql.generators.display_filter import QueryParams
from toolbox.sql.query_executors.sql_query_executor import SqlQueryExecutor
from toolbox.sql.generators import NULL_FILTER_VALUE
from toolbox.server_models import (
    ServerModel,
    FilterParameterNode,
    FilterParametersNode,
)


class Connection:

    def begin_transaction(self):
        return None


# yapf: disable
class MockFilterNode(ServerModel):
    is_private: FilterParameterNode | None = Field(alias='Privacy')
    tribe_ids: FilterParametersNode | None = Field(alias='Tribes')
    tent_ids: FilterParametersNode | None = Field(alias='Tents')
    platforms_ids: FilterParametersNode | None = Field(alias='Platforms')

class DisplayValuesStore:

    @staticmethod
    def get_query_params(field: str) -> QueryParams:
        return {
            'tribe_ids':
                QueryParams(
                    table='tribes_name',
                    value_field='id',
                    display_field='name',
                ),
            'tent_ids':
                QueryParams(
                    table='tents_name',
                    value_field='id',
                    display_field='name',
                ),
            'platforms_ids':
                QueryParams(
                    table='platforms_products',
                    value_field='id',
                    display_field='name',
                ),
        }.get(field)

    @staticmethod
    def get_display_value(field: str, alias: str, value) -> str:
        if field == 'is_private':
            return value
        return ''


class MockSqlQueryExecutor(SqlQueryExecutor):

    def __init__(self) -> None:
        super().__init__(Connection())

    def execute(self, **kwargs):
        query = kwargs['main_query']
        table_name = query.format_params['from']
        return {
            'tribes_name': DataFrame(data={'name': ['XAML United Team']}),
            'tents_name': DataFrame(data={'name': ['WinForms']}),
            'platforms_products': DataFrame(data={'name': []}),
        }[table_name]

@pytest.mark.parametrize(
    'node, output', [
        (
            MockFilterNode(**{
                'Privacy': FilterParameterNode(include=True, value=1),
                'Tribes': FilterParametersNode(include=True, values=['xaml']),
                'Tents': FilterParametersNode(include=False, values=['win'],),
            }),
            [
                ['Privacy', '=', 1],
                'and',
                ['Tribes', 'in', ['XAML United Team']],
                'and',
                [
                    ['Tents', '=', 'NULL'], 'or',
                    ['Tents', 'notin', ['WinForms']]
                ],
            ],
        ),
        (
            MockFilterNode(**{
                'Privacy': FilterParameterNode(include=True, value=0),
                'Tribes': FilterParametersNode(include=False, values=['xaml']),
                'Tents': FilterParametersNode(include=False, values=['win']),
            }),
            [
                ['Privacy', '=', 0],
                'and',
                [
                    ['Tribes', '=', 'NULL'], 'or',
                    ['Tribes', 'notin', ['XAML United Team']]
                ],
                'and',
                [
                    ['Tents', '=', 'NULL'], 'or',
                    ['Tents', 'notin', ['WinForms']]
                ]
            ]
        ),
        (
            MockFilterNode(**{
                'Privacy': FilterParameterNode(include=True, value=0),
                'Platforms': FilterParametersNode(include=True, values=[NULL_FILTER_VALUE]),
            }),
            [
                ['Privacy', '=', 0],
                'and',
                ['Platforms', '=', 'NULL'],
            ]
        ),
        (
            MockFilterNode(**{
                'Privacy': FilterParameterNode(include=True, value=0),
                'Platforms': FilterParametersNode(include=False, values=[NULL_FILTER_VALUE]),
            }),
            [
                ['Privacy', '=', 0],
                'and',
                ['Platforms', '!=', 'NULL'],
            ]
        ),
        (     MockFilterNode(**{
                'Privacy': FilterParameterNode(include=True, value=0),
                'Tribes': FilterParametersNode(include=False, values=['xaml', NULL_FILTER_VALUE]),
            }),
            [
                ['Privacy', '=', 0],
                'and',
                [
                    ['Tribes', '!=', 'NULL'], 'and',
                    ['Tribes', 'notin', ['XAML United Team']]
                ],
            ]
        ),
        (
            MockFilterNode(**{
                'Privacy': FilterParameterNode(include=True, value=100),
                'Tribes': FilterParametersNode(include=True, values=['xaml', NULL_FILTER_VALUE]),
            }),
            [
                ['Privacy', '=', 100],
                'and',
                [
                    ['Tribes', '=', 'NULL'], 'or',
                    ['Tribes', 'in', ['XAML United Team']]
                ],
            ]
        ),
    ]
)
def test_generate_conversion_filter(
    node: MockFilterNode,
    output: list[str | int],
):
    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(DisplayFilterGenerator, '__query_executor', MockSqlQueryExecutor)
        assert DisplayFilterGenerator.__generate_display_filter(
            node=node,
            custom_display_filter=lambda *args: None,
            display_values_store=DisplayValuesStore,
        ) == output
