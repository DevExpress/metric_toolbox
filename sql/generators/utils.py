from collections.abc import Iterable


def build_filter_string(filters: Iterable[str]) -> str:
    return build_multiline_string_ignore_empties(filters)


def build_multiline_string_ignore_empties(
    strings: Iterable[str],
    separator: str = '\n\t',
) -> str:
    return separator.join(filter(None, strings))


def multiline_non_empty(*args: str) -> str:
    return build_multiline_string_ignore_empties(args, separator='\n')
