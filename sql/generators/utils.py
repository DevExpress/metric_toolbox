from collections.abc import Iterable


def build_filter_string(filters: Iterable[str]) -> str:
    return build_multiline_string_ignore_empties(filters)


def build_multiline_string_ignore_empties(strings: Iterable[str]) -> str:
    return '\n\t'.join(filter(None, strings))
