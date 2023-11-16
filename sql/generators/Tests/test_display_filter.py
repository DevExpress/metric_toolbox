import pytest
from pandas import DataFrame
from pydantic import Field
from toolbox.sql.meta_data import KnotMeta
from toolbox.sql.generators.display_filter import QueryParams
from toolbox.sql.query_executors.sql_query_executor import SqlQueryExecutor
from toolbox.sql.generators import NULL_FILTER_VALUE, ge, lt, right_half_open, not_right_half_open, between, notbetwen
from toolbox.server_models import (
    ServerModel,
    FilterParameterNode,
    FilterParametersNode,
)
import toolbox.sql.generators.display_filter as DisplayFilterGenerator


class Connection:

    def begin_transaction(self):
        return None


# yapf: disable
class MockFilterNode(ServerModel):
    is_private: FilterParameterNode | None = Field(alias='Privacy')
    tribe_ids: FilterParametersNode | None = Field(alias='Tribes')
    tent_ids: FilterParametersNode | None = Field(alias='Tents')
    platforms_ids: FilterParametersNode | None = Field(alias='Platforms')
    closed_for: FilterParameterNode | None = Field(alias='Closed for', positive_filter_op=ge, negative_filter_op=lt)
    resolution_time: FilterParametersNode | None = Field(alias='Resolution time', positive_filter_op=right_half_open, negative_filter_op=not_right_half_open)
    closed_between: FilterParametersNode | None = Field(alias='Closed', positive_filter_op=between, negative_filter_op=notbetwen)

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
        return {
            'closed_for': f'{value} day(s)',
            'resolution_time': f'{value} hours(s)',
        }.get(field, value)


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
                'Tents': FilterParametersNode(include=True, values=['win'],),
                'Closed for': FilterParameterNode(include=True, value=12),
                'Resolution time': FilterParametersNode(include=True, values=[1, 3],),
                'Closed': FilterParametersNode(include=True, values=['2023-01-01', '2023-02-01'],),
            }),
            [   ['Privacy', '=', 1],
                'and',
                ['Tribes', 'in', ['XAML United Team']],
                'and',
                ['Tents', 'in', ['WinForms']],
                'and',
                ['Closed for', '>=', '12 day(s)'],
                'and',
                ['Resolution time', '<=<', '[ 1, 3 ) hours(s)'],
                'and',
                ['Closed', 'between', ['2023-01-01', '2023-02-01']],
            ],
        ),
        (
            MockFilterNode(**{
                'Privacy': FilterParameterNode(include=False, value=1),
                'Tribes': FilterParametersNode(include=False, values=['xaml']),
                'Tents': FilterParametersNode(include=False, values=['win'],),
                'Closed for': FilterParameterNode(include=False, value=12),
                'Resolution time': FilterParametersNode(include=False, values=[1, 3],),
                'Closed': FilterParametersNode(include=False, values=['2023-01-01', '2023-02-01'],),
            }),
            [
                ['Privacy', '!=', 1],
                'and',
                [
                    ['Tribes', '=', 'NULL'], 'or',
                    ['Tribes', 'notin', ['XAML United Team']]
                ],
                'and',
                [
                    ['Tents', '=', 'NULL'], 'or',
                    ['Tents', 'notin', ['WinForms']]
                ],
                'and',
                ['Closed for', '<', '12 day(s)'],
                'and',
                [
                    ['Resolution time', '=', 'NULL'], 'or',
                    ['Resolution time', '>=>', '( 1, 3 ] hours(s)']
                ],
                'and',
                [
                    ['Closed', '=', 'NULL'], 'or',
                    ['Closed', 'notbetween', ['2023-01-01', '2023-02-01']],
                ]
            ],
        ),
        (
            MockFilterNode(**{
                'Privacy': FilterParameterNode(include=True, value=0),
                'Tribes': FilterParametersNode(include=False, values=['xaml']),
                'Tents': FilterParametersNode(include=True, values=['win']),
                'Closed for': FilterParameterNode(include=False, value=12),
                'Resolution time': FilterParametersNode(include=True, values=[1, 3],),
                'Closed': FilterParametersNode(include=False, values=['2023-01-01', '2023-02-01'],),
            }),
            [
                ['Privacy', '=', 0],
                'and',
                [
                    ['Tribes', '=', 'NULL'], 'or',
                    ['Tribes', 'notin', ['XAML United Team']]
                ],
                'and',
                ['Tents', 'in', ['WinForms']],
                'and',
                ['Closed for', '<', '12 day(s)'],
                'and',
                ['Resolution time', '<=<', '[ 1, 3 ) hours(s)'],
                'and',
                [
                    ['Closed', '=', 'NULL'], 'or',
                    ['Closed', 'notbetween', ['2023-01-01', '2023-02-01']],
                ]
            ]
        ),
        (
            MockFilterNode(**{
                'Privacy': FilterParameterNode(include=True, value=NULL_FILTER_VALUE),
                'Platforms': FilterParametersNode(include=True, values=[NULL_FILTER_VALUE]),
                'Closed for': FilterParameterNode(include=True, value=NULL_FILTER_VALUE),
                'Resolution time': FilterParametersNode(include=True, values=[1, 3, NULL_FILTER_VALUE],),
                'Closed': FilterParametersNode(include=True, values=['2023-01-01', '2023-02-01', NULL_FILTER_VALUE],),
            }),
            [
                ['Privacy', '=', 'NULL'],
                'and',
                ['Platforms', '=', 'NULL'],
                'and',
                ['Closed for', '=', 'NULL'],
                'and',
                [
                    ['Resolution time', '=', 'NULL'], 'or',
                    ['Resolution time', '<=<', '[ 1, 3 ) hours(s)']
                ],
                'and',
                [
                    ['Closed', '=','NULL'], 'or',
                    ['Closed', 'between', ['2023-01-01', '2023-02-01']]
                ],
            ]
        ),
        (
            MockFilterNode(**{
                'Privacy': FilterParameterNode(include=False, value=NULL_FILTER_VALUE),
                'Platforms': FilterParametersNode(include=False, values=[NULL_FILTER_VALUE]),
                'Closed for': FilterParameterNode(include=False, value=NULL_FILTER_VALUE),
                'Resolution time': FilterParametersNode(include=False, values=[1, 3, NULL_FILTER_VALUE],),
                'Closed': FilterParametersNode(include=False, values=['2023-01-01', '2023-02-01', NULL_FILTER_VALUE],),
            }),
            [
                ['Privacy', '!=', 'NULL'],
                'and',
                ['Platforms', '!=', 'NULL'],
                'and',
                ['Closed for', '!=', 'NULL'],
                'and',
                [
                    ['Resolution time', '!=', 'NULL'], 'and',
                    ['Resolution time', '>=>', '( 1, 3 ] hours(s)']
                ],
                'and',
                [
                    ['Closed', '!=','NULL'], 'and',
                    ['Closed', 'notbetween', ['2023-01-01', '2023-02-01']]
                ],
            ]
        ),
        (
            MockFilterNode(**{
                'Tribes': FilterParametersNode(include=True, values=['xaml', NULL_FILTER_VALUE]),
                'Tents': FilterParametersNode(include=True, values=['win', NULL_FILTER_VALUE],),
            }),
            [
                [
                    ['Tribes', '=', 'NULL'], 'or',
                    ['Tribes', 'in', ['XAML United Team']]
                ],
                'and',
                [
                    ['Tents', '=', 'NULL'], 'or',
                    ['Tents', 'in', ['WinForms']]
                ],
            ],
        ),
        (
            MockFilterNode(**{
                'Tribes': FilterParametersNode(include=False, values=['xaml', NULL_FILTER_VALUE]),
                'Tents': FilterParametersNode(include=True, values=['win', NULL_FILTER_VALUE],),
            }),
            [
                [
                    ['Tribes', '!=', 'NULL'], 'and',
                    ['Tribes', 'notin', ['XAML United Team']]
                ],
                'and',
                [
                    ['Tents', '=', 'NULL'], 'or',
                    ['Tents', 'in', ['WinForms']]
                ],
            ],
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


# yapf: enable
def test_query_params_defaults():
    qp = QueryParams(table='tents_name')
    assert qp.display_field == KnotMeta.name.name
    assert qp.value_field == KnotMeta.id.name
