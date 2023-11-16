from typing import Literal


NULL_FILTER_VALUE = '#_NULL_FILTER_VALUE_#'
null_filter_type = Literal['#_NULL_FILTER_VALUE_#']

from toolbox.sql.generators.filter_operations import (
    between,
    notbetwen,
    eq,
    ne,
    ge,
    lt,
    right_half_open,
    not_right_half_open,
    in_,
    notin,
)
